import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import traceback
import csv
from decimal import Decimal
from typing import Tuple

# --- 導入新的客戶端 ---
from exchanges.grvt import GrvtClient
from exchanges.paradex import ParadexClient
import websockets
from datetime import datetime
import pytz

# 確保可以找到 exchanges 模組 (必須在文件開頭)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- 策略常數 ---
HEDGE_TIMEOUT = 10  # Taker 腿對沖超時時間 (秒)
HOLDING_TIME = 180  # 預期持倉時間 (5 分鐘)
MAX_RISK_USD = Decimal('30')  # 最大可容忍淨浮動虧損 (USD)


class Config:
    """Simple config class to wrap dictionary for exchange clients."""

    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """Trading bot that places post-only orders on GRVT and hedges with market orders on Paradex."""

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5, iterations: int = 20,
                 start_side: str = 'buy'):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.start_side = start_side
        self.current_side = start_side

        # --- 倉位與價格狀態 ---
        self.grvt_position = Decimal('0')
        self.paradex_position = Decimal('0')
        self.grvt_open_price = Decimal('0')
        self.paradex_open_price = Decimal('0')
        self.open_time = 0.0
        self.current_net_pnl = Decimal('0')

        # --- 邏輯狀態旗標 ---
        self.is_closing = False  # 保持此旗標，用於 WS 處理

        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/grvt_paradex_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/grvt_paradex_{ticker}_hedge_mode_trades.csv"

        self._initialize_csv_file()
        self._setup_logger()

        self.stop_flag = False
        self.order_counter = 0

        # --- GRVT (Maker) State ---
        self.grvt_client = None
        self.grvt_contract_id = None
        self.grvt_tick_size = None
        self.grvt_order_status = None
        self.grvt_best_bid = None
        self.grvt_best_ask = None

        # --- Paradex (Taker) State ---
        self.paradex_client = None
        self.paradex_contract_id = None
        self.paradex_tick_size = None
        self.paradex_best_bid = None
        self.paradex_best_ask = None

        # --- 策略狀態 ---
        self.waiting_for_paradex_fill = False
        self.order_execution_complete = False
        self.current_paradex_side = None
        self.current_paradex_quantity = None

        # --- PNL 監控任務 ---
        self.pnl_monitor_task = None

    def _setup_logger(self):
        """Setup logging configuration."""
        self.logger = logging.getLogger(f"hedge_bot_{self.ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        if self.pnl_monitor_task and not self.pnl_monitor_task.done():
            self.pnl_monitor_task.cancel()
            self.logger.info("🔌 PNL monitor task cancelled")

        if self.grvt_client: self.logger.info("🔌 GRVT WebSocket will be disconnected")
        if self.paradex_client: self.logger.info("🔌 Paradex WebSocket will be disconnected")

        for handler in self.logger.handlers[:]:
            try:
                handler.close()
            except Exception:
                pass

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file."""
        timestamp = datetime.now(pytz.UTC).isoformat()
        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([exchange, timestamp, side, price, quantity])
        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    # --- Paradex 對沖成交後結果處理 ---
    def handle_paradex_hedge_result(self, order_data):
        """Handle Paradex hedge order result after it's confirmed FILLED."""
        try:
            side = order_data.get('side', '').upper()
            filled_size = Decimal(order_data.get('filled_size', '0'))
            avg_price = Decimal(order_data.get('price', '0'))

            if filled_size == 0: return

            # 正常更新倉位
            if side == "SELL":
                self.paradex_position -= filled_size
            else:  # BUY
                self.paradex_position += filled_size

            self.paradex_open_price = avg_price

            self.logger.info(f"📊 Paradex hedge FILLED: {side} {filled_size} @ {avg_price}")
            self.log_trade_to_csv(exchange='Paradex', side=side, price=str(avg_price), quantity=str(filled_size))
            self.order_execution_complete = True

            # 🚨 關鍵：平倉時，將 Paradex 倉位強制設為 0
            if self.is_closing:
                self.paradex_position = Decimal('0')
                self.logger.info("✅ Paradex Taker 倉位已強制清零 (平倉完成)")


        except Exception as e:
            self.logger.error(f"Error handling Paradex hedge result: {e}")

    # --- 步驟 2 輔助函式：GRVT 市價平倉單邊倉位 ---
    async def close_grvt_market_position(self, direction: str, quantity: Decimal):
        """Place a Market Order on GRVT (using place_open_order fallback) to close the remaining single-side position."""
        if not self.grvt_client: return

        self.logger.critical(
            f"🚨 Executing EMERGENCY MARKET CLOSE on GRVT (Fallback to aggressive Limit): {direction} {quantity}")

        try:
            # 🌟 修正：使用 place_open_order 進行積極的 Taker 執行
            order_result = await self.grvt_client.place_open_order(
                contract_id=self.grvt_contract_id,
                quantity=quantity,
                direction=direction.lower()
            )

            if order_result.success:
                # 🚨 關鍵修正：Market Close 應在成功發送後立即確認倉位清零
                self.grvt_position = Decimal('0')
                self.logger.critical("✅ GRVT單邊倉位已成功發送平倉，內部狀態已清零。")
            else:
                self.logger.critical(f"❌ 嚴重錯誤：GRVT平倉失敗: {order_result.error_message}")
                self.stop_flag = True

        except Exception as e:
            self.logger.critical(f"❌ 嚴重錯誤：GRVT平倉異常: {e}")
            self.stop_flag = True

    # --- Paradex Taker 執行邏輯 ---
    async def place_paradex_market_order(self, paradex_side: str, quantity: Decimal):
        """Place a Market Order on Paradex for immediate hedge."""
        if not self.paradex_client: return False

        self.logger.info(f"🚀 Placing Paradex MARKET order: {paradex_side} {quantity}")

        order_result = await self.paradex_client.place_market_order(
            contract_id=self.paradex_contract_id,
            quantity=quantity,
            direction=paradex_side.lower()
        )

        if not order_result.success:
            self.logger.error(f"❌ Paradex Taker order failed: {order_result.error_message}")
            return False

        order_info = await self.monitor_paradex_hedge_order(order_result.order_id)

        if order_info and order_info.status == 'FILLED':
            self.handle_paradex_hedge_result({
                'side': order_info.side,
                'filled_size': order_info.filled_size,
                'price': order_info.price
            })
            return True
        else:
            self.logger.error(f"❌ Paradex Taker 對沖失敗或超時 ({HEDGE_TIMEOUT}s)")

            grvt_side_to_close = 'sell' if self.grvt_position > 0 else 'buy'
            await self.close_grvt_market_position(grvt_side_to_close, abs(self.grvt_position))

            return False

    async def monitor_paradex_hedge_order(self, order_id: str):
        """Monitor Paradex Taker order status until FILLED or timeout."""
        start_time = time.time()

        while time.time() - start_time < HEDGE_TIMEOUT:
            await asyncio.sleep(0.5)
            order_info = await self.paradex_client.get_order_info(order_id=order_id)

            if order_info and order_info.status == 'FILLED':
                return order_info
            elif order_info and order_info.status in ['CANCELED', 'REJECTED']:
                self.logger.error(f"❌ Paradex hedge order REJECTED/CANCELED: {order_info.status}")
                break

        return None

    # --- PNL 監控函式 ---
    async def calculate_and_monitor_pnl(self):
        # ... (PNL 邏輯不變) ...
        while not self.stop_flag:
            await asyncio.sleep(0.5)

            if self.grvt_position == 0 or self.paradex_position == 0 or self.open_time == 0:
                self.current_net_pnl = Decimal('0')
                continue

            try:
                grvt_bid, grvt_ask = await self.grvt_client.fetch_bbo_prices(self.grvt_contract_id)
                paradex_bid, paradex_ask = await self.paradex_client.fetch_bbo_prices(self.paradex_contract_id)

                if grvt_bid == 0 or paradex_bid == 0 or grvt_bid >= grvt_ask or paradex_bid >= paradex_ask: continue

                if self.grvt_position > 0:
                    grvt_pnl = self.grvt_position * (grvt_bid - self.grvt_open_price)
                else:
                    grvt_pnl = self.grvt_position * (self.grvt_open_price - grvt_ask)

                if self.paradex_position > 0:
                    paradex_pnl = self.paradex_position * (paradex_bid - self.paradex_open_price)
                else:
                    paradex_pnl = self.paradex_position * (self.paradex_open_price - paradex_ask)

                net_pnl = grvt_pnl + paradex_pnl
                self.current_net_pnl = net_pnl

                if net_pnl < -MAX_RISK_USD:
                    self.logger.critical(f"🔥🔥🔥 風險值突破！淨浮動虧損達到 {net_pnl:.2f} USD 🔥🔥🔥")
                    self.open_time = 0
                    return

            except Exception as e:
                self.logger.error(f"❌ PNL 計算錯誤: {e}")

    # --- 客戶端初始化 ---
    def initialize_grvt_client(self):
        if self.grvt_client is None:
            config_dict = {'ticker': self.ticker, 'contract_id': '', 'quantity': self.order_quantity,
                           'tick_size': Decimal('0.01'), 'close_order_side': 'sell'}
            config = Config(config_dict)
            self.grvt_client = GrvtClient(config)
            self.logger.info("✅ GRVT Maker client initialized successfully")
        return self.grvt_client

    def initialize_paradex_client(self):
        if self.paradex_client is None:
            config_dict = {'ticker': self.ticker, 'contract_id': '', 'quantity': self.order_quantity,
                           'tick_size': Decimal('0.01'), 'close_order_side': 'sell'}
            config = Config(config_dict)
            self.paradex_client = ParadexClient(config)
            self.logger.info("✅ Paradex Taker client initialized successfully")
        return self.paradex_client

    # --- 獲取合約資訊 ---
    async def get_contract_info(self):
        self.grvt_contract_id, self.grvt_tick_size = await self.grvt_client.get_contract_attributes()
        self.paradex_contract_id, self.paradex_tick_size = await self.paradex_client.get_contract_attributes()

        if self.order_quantity < self.grvt_client.config.quantity or self.order_quantity < self.paradex_client.config.quantity:
            raise ValueError("Order quantity is less than minimum quantity on one of the exchanges.")

    # --- 🌟 修正點: 外部倉位獲取 (用於校準) ---
    async def fetch_current_exchange_positions(self):
        """Fetch real-time positions from both exchanges to correct internal state."""
        try:
            grvt_real_pos = await self.grvt_client.get_account_positions()  # 假設返回淨頭寸
            paradex_real_pos = await self.paradex_client.get_account_positions()

            # 只有當實際倉位遠小於一個訂單量時才清零內部狀態
            if abs(grvt_real_pos) < self.grvt_client.config.quantity * Decimal('0.5'):
                self.grvt_position = Decimal('0')
            if abs(paradex_real_pos) < self.paradex_client.config.quantity * Decimal('0.5'):
                self.paradex_position = Decimal('0')

            self.logger.info(f"🔄 外部倉位檢查完成。GRVT: {self.grvt_position}, Paradex: {self.paradex_position}")

        except Exception as e:
            self.logger.error(f"❌ 無法獲取外部倉位: {e}")

    # --- GRVT Post-Only 輔助函式 (僅開倉) ---
    async def place_grvt_open_order(self, side: str, quantity: Decimal):
        """Place an open order on GRVT using client's logic (includes Post-Only retry)."""
        best_bid, best_ask = await self.grvt_client.fetch_bbo_prices(self.grvt_contract_id)

        order_result = await self.grvt_client.place_open_order(
            contract_id=self.grvt_contract_id,
            quantity=quantity,
            direction=side.lower()
        )

        if order_result.success:
            return order_result.order_id, order_result.price
        else:
            raise Exception(f"Failed to place order: {order_result.error_message}")

    # --- 雙邊平倉 ---
    async def close_both_positions(self):
        """Execute the final symmetrical close on both exchanges."""
        self.logger.info("🕒 執行雙邊平倉...")

        grvt_qty = abs(self.grvt_position)
        paradex_qty = abs(self.paradex_position)

        if grvt_qty == 0 and paradex_qty == 0:
            self.logger.warning("倉位已經為零，跳過平倉。")
            return

        # 🚨 關鍵：設置平倉狀態旗標，讓 WS Handler 知道這是平倉
        self.is_closing = True

        grvt_close_side = 'sell' if self.grvt_position > 0 else 'buy'
        paradex_close_side = 'sell' if self.paradex_position > 0 else 'buy'

        # 1. GRVT (Market) 平倉 -> 🌟 修正點：改為 Market Close
        self.logger.info(f"Closing GRVT Market: {grvt_close_side} {grvt_qty}")
        await self.close_grvt_market_position(grvt_close_side, grvt_qty)  # 調用 Market Close

        # 2. Paradex (Taker) 平倉 Market 訂單
        self.logger.info(f"Closing Paradex Taker: {paradex_close_side} {paradex_qty}")
        await self.place_paradex_market_order(paradex_close_side, paradex_qty)

        # 🚨 移除手動清零，但等待 WS 完成
        self.logger.info("✅ 雙邊平倉指令已發送完成，等待 WS 確認清零。")
        self.order_execution_complete = True

        # 🚨 關鍵：平倉後，強制等待 5 秒，確保所有平倉信號傳回
        await asyncio.sleep(5)
        self.is_closing = False  # 重置旗標

    # --- GRVT 訂單更新處理 (Websocket Handler) ---
    def handle_grvt_order_update(self, order_data):
        """Handle GRVT order updates from WebSocket."""

        # 🚨 修正點: 緊急停止檢查 and 平倉時忽略開倉邏輯
        if self.stop_flag:
            self.logger.warning("Bot is shutting down, ignoring incoming FILLED order to prevent hedge.")
            return

        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))
        price = Decimal(order_data.get('price', '0'))
        status = order_data.get('status')

        if status != 'FILLED':
            self.grvt_order_status = status
            return

        # 判斷是否為平倉成交
        if self.is_closing:
            # 這是平倉訂單成交
            self.logger.info(f"✅ GRVT 平倉成交: {side} {filled_size} @ {price} [Cleaned]")
            self.log_trade_to_csv(exchange='GRVT', side=f"CLOSE_{side}", price=str(price), quantity=str(filled_size))

            # 🚨 核心修正：平倉成交，將 GRVT 倉位設為 0
            self.grvt_position = Decimal('0')
            self.is_closing = False  # 重置旗標
            return

            # --- 開倉邏輯 (ONLY IF NOT CLOSING) ---
        if side == 'buy':
            self.grvt_position += filled_size
            paradex_side = 'sell'
        else:
            self.grvt_position -= filled_size
            paradex_side = 'buy'

        self.grvt_open_price = price
        self.grvt_order_status = 'FILLED'

        self.log_trade_to_csv(exchange='GRVT', side=side, price=str(price), quantity=str(filled_size))

        self.current_paradex_side = paradex_side
        self.current_paradex_quantity = filled_size
        self.waiting_for_paradex_fill = True
        self.logger.info(f"📋 Ready to place Paradex hedge order: {paradex_side} {filled_size} @ {price}")

    # --- Setup GRVT/Paradex WebSockets ---
    async def setup_clients_websocket(self):
        """Setup both GRVT (order updates) and Paradex (depth/order updates) websockets."""
        self.grvt_client.setup_order_update_handler(self.handle_grvt_order_update)
        await self.grvt_client.connect()
        self.logger.info("✅ GRVT Maker WebSocket connected")

        await self.paradex_client.connect()
        self.logger.info("✅ Paradex Taker WebSocket connected")

        await asyncio.sleep(2)

    # --- 核心交易循環 (Trading Loop) ---
    async def trading_loop(self):
        self.logger.info(f"🚀 Starting GRVT/Paradex hedge bot for {self.ticker}")

        try:
            self.initialize_grvt_client()
            self.initialize_paradex_client()
            await self.get_contract_info()
            self.logger.info(f"Contract info loaded - GRVT: {self.grvt_contract_id}, Paradex: {self.paradex_contract_id}")
            await self.setup_clients_websocket()
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        iterations = 0
        while iterations < self.iterations and not self.stop_flag:
            iterations += 1

            # 決定開倉方向 (修正交替邏輯)
            if iterations == 1:
                side = self.start_side
            else:
                side = 'buy' if self.current_side == 'sell' else 'sell'
            self.current_side = side

            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}. Net P&L: {self.current_net_pnl:.2f} USD")
            self.logger.info("-----------------------------------------------")

            if abs(self.grvt_position + self.paradex_position) > self.order_quantity * Decimal('0.1'):
                self.logger.critical(f"❌ 倉位差異過大: {self.grvt_position + self.paradex_position}. 停止交易。")
                break

            # --- 倉位清零校準檢查 ---
            if self.grvt_position != 0 or self.paradex_position != 0:
                self.logger.warning("倉位未清零，進行外部校準並跳過開倉。")
                await self.fetch_current_exchange_positions()
                await asyncio.sleep(5)
                continue

            # --- 階段 1: 開倉與對沖 (Maker: GRVT) ---
            self.is_closing = False  # 確保進入開倉階段時旗標為 False
            self.order_execution_complete = False
            self.waiting_for_paradex_fill = False
            self.grvt_order_status = None

            try:
                await self.place_grvt_open_order(side, self.order_quantity)

                start_time = time.time()
                while not self.waiting_for_paradex_fill and not self.stop_flag and (time.time() - start_time < 180):
                    await asyncio.sleep(0.1)

                if self.waiting_for_paradex_fill and not self.stop_flag:
                    self.logger.info("GRVT filled. Executing Paradex hedge...")
                    hedge_success = await self.place_paradex_market_order(
                        self.current_paradex_side,
                        self.current_paradex_quantity
                    )
                    if not hedge_success and self.grvt_position != 0:
                        self.logger.warning("Hedge/Emergency Close Failed. Retrying open next cycle.")
                        continue
                elif not self.stop_flag and self.grvt_order_status not in ['FILLED', 'CANCELED']:
                    self.logger.warning("GRVT Maker order timeout. Canceling and retrying...")
                    await self.grvt_client.cancel_all_orders(self.grvt_contract_id)
                    continue

            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                break

            # --- 階段 2: 5 分鐘持倉等待 (含 PNL 風險控制) ---
            if self.grvt_position != 0 or self.paradex_position != 0:  # 確保至少有一邊有倉位
                self.open_time = time.time()
                self.pnl_monitor_task = asyncio.create_task(self.calculate_and_monitor_pnl())

                self.logger.info(f"⏳ 雙邊對沖成功建立。等待 {HOLDING_TIME} 秒 (或 PNL 觸發)...")

                while time.time() < self.open_time + HOLDING_TIME and not self.stop_flag and self.open_time != 0:
                    await asyncio.sleep(1)

                if self.pnl_monitor_task:
                    self.pnl_monitor_task.cancel()

                if self.open_time == 0 and not self.stop_flag:
                    self.logger.info("⚠️ PNL 監控觸發緊急平倉，立即執行。")
                else:
                    self.logger.info(f"✅ 5分鐘持倉時間已到。執行平倉。")

            # --- 階段 3: 雙邊平倉 ---
            if self.grvt_position != 0 or self.paradex_position != 0 and not self.stop_flag:
                await self.close_both_positions()

            # --- 倉位清零最終確認 ---
            start_time = time.time()
            self.is_closing = True  # 設置平倉狀態，讓 WS 忽略成交觸發
            while (self.grvt_position != 0 or self.paradex_position != 0) and not self.stop_flag and (
                    time.time() - start_time < 30):
                await self.fetch_current_exchange_positions()  # 外部校準

                self.logger.info(f"🔄 等待平倉確認... GRVT: {self.grvt_position}, Paradex: {self.paradex_position}")
                await asyncio.sleep(2)

            if self.grvt_position != 0 or self.paradex_position != 0:
                self.logger.critical("🚨 嚴重錯誤：平倉後倉位未清零。手動介入！")
                self.stop_flag = True

            await asyncio.sleep(10)  # 修正點：平倉後等待 10 秒才進入下一個循環

    async def run(self):
        """強制定義 run() 方法，避免 AttributeError."""
        self.setup_signal_handlers()
        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
