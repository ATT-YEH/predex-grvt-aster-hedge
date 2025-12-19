import asyncio
import signal
import logging
import os
import sys
import time
import csv
from decimal import Decimal
from typing import Optional, Dict, Any, Tuple

from exchanges.grvt import GrvtClient
from exchanges.lighter import LighterClient
from datetime import datetime
import pytz

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HEDGE_TIMEOUT = 10
HOLDING_TIME = 180
MAX_RISK_USD = Decimal('30')


class Config:
    """Simple config class to wrap dictionary for exchange clients."""

    def __init__(self, config_dict: Dict[str, Any]):
        for key, value in config_dict.items():
            setattr(self, key, value)


def _normalize_order_result(res: Any) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Normalize different return types into:
      (ok, order_id, error_message)

    Supports:
      - OrderResult: has .success, .order_id, .error_message
      - OrderInfo: has .order_id, .status
      - dict: keys may be success/order_id/error_message/id/client_order_index
    """
    if res is None:
        return False, None, "order result is None"

    if isinstance(res, dict):
        ok = bool(res.get("success", True))
        oid = (
            res.get("order_id")
            or res.get("id")
            or res.get("client_order_index")
            or res.get("clientOrderIndex")
            or res.get("client_order_id")
            or res.get("clientOrderId")
        )
        err = res.get("error_message") or res.get("error") or res.get("message")
        if ok is False and not err:
            err = "order failed (dict)"
        return ok, str(oid) if oid is not None else None, err

    if hasattr(res, "success"):
        ok = bool(getattr(res, "success"))
        oid = getattr(res, "order_id", None)
        err = getattr(res, "error_message", None)
        return ok, str(oid) if oid is not None else None, err

    if hasattr(res, "order_id"):
        oid = getattr(res, "order_id", None)
        return True, str(oid) if oid is not None else None, None

    return False, None, f"unknown order result type: {type(res)}"


class HedgeBot:
    """Trading bot that places post-only orders on GRVT and hedges with market orders on Lighter."""

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int = 5,
        iterations: int = 20,
        start_side: str = 'buy',
        holding_time: int = HOLDING_TIME,
        hedge_timeout: int = HEDGE_TIMEOUT,
        max_risk_usd: Decimal = MAX_RISK_USD,
        sleep_between_cycles: float = 10.0,
        open_wait_timeout: Optional[int] = None,
        grvt_force_market: bool = False,
    ):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.start_side = start_side
        self.current_side = start_side
        self.holding_time = holding_time
        self.hedge_timeout = hedge_timeout
        self.max_risk_usd = Decimal(str(max_risk_usd))
        self.sleep_between_cycles = sleep_between_cycles
        self.open_wait_timeout = open_wait_timeout if open_wait_timeout is not None else fill_timeout
        self.grvt_force_market = grvt_force_market

        self.grvt_position = Decimal('0')
        self.lighter_position = Decimal('0')
        self.grvt_open_price = Decimal('0')
        self.lighter_open_price = Decimal('0')
        self.open_time = 0.0
        self.current_net_pnl = Decimal('0')

        self.is_closing = False

        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/grvt_lighter_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/grvt_lighter_{ticker}_hedge_mode_trades.csv"

        self._initialize_csv_file()
        self._setup_logger()

        self.stop_flag = False
        self.order_counter = 0

        self.grvt_client = None
        self.grvt_contract_id = None
        self.grvt_tick_size = None
        self.grvt_order_status = None
        self.grvt_best_bid = None
        self.grvt_best_ask = None
        self.current_grvt_order_id = None

        self.lighter_client = None
        self.lighter_contract_id = None
        self.lighter_tick_size = None
        self.lighter_best_bid = None
        self.lighter_best_ask = None

        self.waiting_for_lighter_fill = False
        self.order_execution_complete = False
        self.current_lighter_side = None
        self.current_lighter_quantity = None
        self.current_lighter_client_order_id = None

        self.pnl_monitor_task = None

    def _setup_logger(self):
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
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        if self.pnl_monitor_task and not self.pnl_monitor_task.done():
            self.pnl_monitor_task.cancel()
            self.logger.info("🔌 PNL monitor task cancelled")

        if self.grvt_client:
            self.logger.info("🔌 GRVT WebSocket will be disconnected")
        if self.lighter_client:
            self.logger.info("🔌 Lighter WebSocket will be disconnected")

        for handler in self.logger.handlers[:]:
            try:
                handler.close()
            except Exception:
                pass

    def _initialize_csv_file(self):
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        timestamp = datetime.now(pytz.UTC).isoformat()
        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([exchange, timestamp, side, price, quantity])
        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    def handle_lighter_hedge_result(self, order_data):
        try:
            side = order_data.get('side', '').upper()
            filled_size = Decimal(order_data.get('filled_size', '0'))
            avg_price = Decimal(order_data.get('price', '0'))

            if filled_size == 0:
                return

            if side == "SELL":
                self.lighter_position -= filled_size
            else:
                self.lighter_position += filled_size

            self.lighter_open_price = avg_price

            self.logger.info(f"📊 Lighter hedge FILLED: {side} {filled_size} @ {avg_price}")
            self.log_trade_to_csv(exchange='Lighter', side=side, price=str(avg_price), quantity=str(filled_size))
            self.order_execution_complete = True

            if self.is_closing:
                self.lighter_position = Decimal('0')
                self.logger.info("✅ Lighter Taker 倉位已強制清零 (平倉完成)")

        except Exception as e:
            self.logger.error(f"Error handling Lighter hedge result: {e}")

    async def close_grvt_market_position(self, direction: str, quantity: Decimal):
        if not self.grvt_client:
            return

        self.logger.critical(
            f"🚨 Executing EMERGENCY MARKET CLOSE on GRVT (Fallback to aggressive Limit): {direction} {quantity}")

        try:
            order_result = await self.grvt_client.place_open_order(
                contract_id=self.grvt_contract_id,
                quantity=quantity,
                direction=direction.lower()
            )

            ok, _, err = _normalize_order_result(order_result)
            if ok:
                self.grvt_position = Decimal('0')
                self.logger.critical("✅ GRVT單邊倉位已成功發送平倉，內部狀態已清零。")
            else:
                self.logger.critical(f"❌ 嚴重錯誤：GRVT平倉失敗: {err}")
                self.stop_flag = True

        except Exception as e:
            self.logger.critical(f"❌ 嚴重錯誤：GRVT平倉異常: {e}")
            self.stop_flag = True

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal):
        if not self.lighter_client:
            return False

        self.logger.info(f"🚀 Placing Lighter MARKET order: {lighter_side} {quantity}")
        self.current_lighter_client_order_id = None

        try:
            try:
                order_result = await self.lighter_client.place_market_order(
                    contract_id=self.lighter_contract_id,
                    quantity=quantity,
                    direction=lighter_side.lower()
                )
            except TypeError:
                order_result = await self.lighter_client.place_market_order(
                    contract_id=self.lighter_contract_id,
                    quantity=quantity,
                    side=lighter_side.lower()
                )

            if order_result is None:
                self.logger.error("❌ Lighter Taker order failed: order_result is None")
                grvt_side_to_close = 'sell' if self.grvt_position > 0 else 'buy'
                await self.close_grvt_market_position(grvt_side_to_close, abs(self.grvt_position))
                return False

        except Exception:
            self.logger.error("❌ Lighter Taker order exception", exc_info=True)
            grvt_side_to_close = 'sell' if self.grvt_position > 0 else 'buy'
            await self.close_grvt_market_position(grvt_side_to_close, abs(self.grvt_position))
            return False

        ok, oid, err = _normalize_order_result(order_result)
        if not ok:
            self.logger.error(f"❌ Lighter Taker order failed: {err}")
            grvt_side_to_close = 'sell' if self.grvt_position > 0 else 'buy'
            await self.close_grvt_market_position(grvt_side_to_close, abs(self.grvt_position))
            return False

        if oid is None:
            self.logger.error("❌ Lighter hedge returned no order_id/client_order_index; cannot track fills.")
            grvt_side_to_close = 'sell' if self.grvt_position > 0 else 'buy'
            await self.close_grvt_market_position(grvt_side_to_close, abs(self.grvt_position))
            return False

        self.current_lighter_client_order_id = str(oid)
        self.waiting_for_lighter_fill = True
        self.logger.info(f"🧾 Lighter hedge sent. client_order_index={self.current_lighter_client_order_id}")

        hedge_filled = await self.wait_lighter_fill_or_timeout()
        if hedge_filled:
            return True

        self.logger.error(f"❌ Lighter Taker 對沖失敗或超時 ({self.hedge_timeout}s)")
        grvt_side_to_close = 'sell' if self.grvt_position > 0 else 'buy'
        await self.close_grvt_market_position(grvt_side_to_close, abs(self.grvt_position))
        return False

    async def wait_lighter_fill_or_timeout(self) -> bool:
        start_time = time.time()

        while time.time() - start_time < self.hedge_timeout:
            if not self.waiting_for_lighter_fill:
                return True
            if self.stop_flag:
                break
            await asyncio.sleep(0.1)

        self.waiting_for_lighter_fill = False
        return False

    async def calculate_and_monitor_pnl(self):
        while not self.stop_flag:
            await asyncio.sleep(0.5)

            if self.grvt_position == 0 or self.lighter_position == 0 or self.open_time == 0:
                self.current_net_pnl = Decimal('0')
                continue

            try:
                grvt_bid, grvt_ask = await self.grvt_client.fetch_bbo_prices(self.grvt_contract_id)
                lighter_bid, lighter_ask = await self.lighter_client.fetch_bbo_prices(self.lighter_contract_id)

                if grvt_bid == 0 or lighter_bid == 0 or grvt_bid >= grvt_ask or lighter_bid >= lighter_ask:
                    continue

                if self.grvt_position > 0:
                    grvt_pnl = self.grvt_position * (grvt_bid - self.grvt_open_price)
                else:
                    grvt_pnl = self.grvt_position * (self.grvt_open_price - grvt_ask)

                if self.lighter_position > 0:
                    lighter_pnl = self.lighter_position * (lighter_bid - self.lighter_open_price)
                else:
                    lighter_pnl = self.lighter_position * (self.lighter_open_price - lighter_ask)

                net_pnl = grvt_pnl + lighter_pnl
                self.current_net_pnl = net_pnl

                if net_pnl < -self.max_risk_usd:
                    self.logger.critical(f"🔥🔥🔥 風險值突破！淨浮動虧損達到 {net_pnl:.2f} USD 🔥🔥🔥")
                    self.open_time = 0
                    return

            except Exception as e:
                self.logger.error(f"❌ PNL 計算錯誤: {e}")

    def initialize_grvt_client(self):
        if self.grvt_client is None:
            config_dict = {'ticker': self.ticker, 'contract_id': '', 'quantity': self.order_quantity,
                           'tick_size': Decimal('0.01'), 'close_order_side': 'sell'}
            config = Config(config_dict)
            self.grvt_client = GrvtClient(config)
            self.logger.info("✅ GRVT Maker client initialized successfully")
        return self.grvt_client

    def initialize_lighter_client(self):
        if self.lighter_client is None:
            config_dict = {'ticker': self.ticker, 'contract_id': '', 'quantity': self.order_quantity,
                           'tick_size': Decimal('0.01'), 'close_order_side': 'sell'}
            config = Config(config_dict)
            self.lighter_client = LighterClient(config)
            self.logger.info("✅ Lighter Taker client initialized successfully")
        return self.lighter_client

    async def get_contract_info(self):
        self.grvt_contract_id, self.grvt_tick_size = await self.grvt_client.get_contract_attributes()
        self.lighter_contract_id, self.lighter_tick_size = await self.lighter_client.get_contract_attributes()

        if self.order_quantity < self.grvt_client.config.quantity or self.order_quantity < self.lighter_client.config.quantity:
            raise ValueError("Order quantity is less than minimum quantity on one of the exchanges.")

    async def fetch_current_exchange_positions(self):
        try:
            grvt_real_pos = await self.grvt_client.get_account_positions()
            lighter_real_pos = await self.lighter_client.get_account_positions()

            if abs(grvt_real_pos) < self.grvt_client.config.quantity * Decimal('0.5'):
                self.grvt_position = Decimal('0')
            if abs(lighter_real_pos) < self.lighter_client.config.quantity * Decimal('0.5'):
                self.lighter_position = Decimal('0')

            self.logger.info(f"🔄 外部倉位檢查完成。GRVT: {self.grvt_position}, Lighter: {self.lighter_position}")

        except Exception as e:
            self.logger.error(f"❌ 無法獲取外部倉位: {e}")

    async def place_grvt_open_order(self, side: str, quantity: Decimal):
        if self.grvt_force_market:
            self.logger.warning(f"⚠️ GRVT FORCE MARKET enabled: {side} {quantity}")
            try:
                await self.grvt_client.place_market_order(
                    contract_id=self.grvt_contract_id,
                    quantity=quantity,
                    side=side.lower()
                )
            except Exception as e:
                raise Exception(f"Failed to place GRVT market order: {e}")

            self.grvt_order_status = "OPEN"
            self.current_grvt_order_id = None
            return None, None

        order_result = await self.grvt_client.place_open_order(
            contract_id=self.grvt_contract_id,
            quantity=quantity,
            direction=side.lower()
        )

        ok, oid, err = _normalize_order_result(order_result)
        if ok:
            self.current_grvt_order_id = oid
            return oid, getattr(order_result, 'price', None)
        raise Exception(f"Failed to place order: {err}")

    async def close_both_positions(self):
        self.logger.info("🕒 執行雙邊平倉...")

        grvt_qty = abs(self.grvt_position)
        lighter_qty = abs(self.lighter_position)

        if grvt_qty == 0 and lighter_qty == 0:
            self.logger.warning("倉位已經為零，跳過平倉。")
            return

        self.is_closing = True

        grvt_close_side = 'sell' if self.grvt_position > 0 else 'buy'
        lighter_close_side = 'sell' if self.lighter_position > 0 else 'buy'

        self.logger.info(f"Closing GRVT Market: {grvt_close_side} {grvt_qty}")
        await self.close_grvt_market_position(grvt_close_side, grvt_qty)

        self.logger.info(f"Closing Lighter Taker: {lighter_close_side} {lighter_qty}")
        await self.place_lighter_market_order(lighter_close_side, lighter_qty)

        self.logger.info("✅ 雙邊平倉指令已發送完成，等待 WS 確認清零。")
        self.order_execution_complete = True

        await asyncio.sleep(5)
        self.is_closing = False

    def handle_grvt_order_update(self, order_data):
        if self.stop_flag:
            self.logger.warning("Bot is shutting down, ignoring incoming FILLED order to prevent hedge.")
            return

        updates = order_data if isinstance(order_data, list) else [order_data]
        for update in updates:
            if not isinstance(update, dict):
                continue

            side = update.get('side', '').lower()
            filled_size = Decimal(update.get('filled_size', '0'))
            price = Decimal(update.get('price', '0'))
            status = update.get('status')

            if status != 'FILLED':
                self.grvt_order_status = status
                continue

            if self.is_closing:
                self.logger.info(f"✅ GRVT 平倉成交: {side} {filled_size} @ {price} [Cleaned]")
                self.log_trade_to_csv(exchange='GRVT', side=f"CLOSE_{side}", price=str(price),
                                      quantity=str(filled_size))

                self.grvt_position = Decimal('0')
                self.is_closing = False
                return

            if side == 'buy':
                self.grvt_position += filled_size
                lighter_side = 'sell'
            else:
                self.grvt_position -= filled_size
                lighter_side = 'buy'

            self.grvt_open_price = price
            self.grvt_order_status = 'FILLED'

            self.log_trade_to_csv(exchange='GRVT', side=side, price=str(price), quantity=str(filled_size))

            self.current_lighter_side = lighter_side
            self.current_lighter_quantity = filled_size
            self.waiting_for_lighter_fill = True
            self.logger.info(f"📋 Ready to place Lighter hedge order: {lighter_side} {filled_size} @ {price}")

    def handle_lighter_order_update(self, order_data):
        updates = order_data if isinstance(order_data, list) else [order_data]
        for update in updates:
            if not isinstance(update, dict):
                continue

            status = str(update.get('status', '')).upper()
            is_ask = bool(update.get('is_ask', False))
            side = 'sell' if is_ask else 'buy'

            client_order_index = update.get('client_order_index', None)
            filled_base_amount = Decimal(str(update.get('filled_base_amount', 0) or 0))
            price = Decimal(str(update.get('price', 0) or 0))

            if not self.waiting_for_lighter_fill:
                continue
            if self.current_lighter_client_order_id is not None:
                if str(client_order_index) != str(self.current_lighter_client_order_id):
                    continue

            if status == "OPEN" and filled_base_amount > 0:
                status = "PARTIALLY_FILLED"

            if status == "FILLED" and filled_base_amount > 0:
                self.waiting_for_lighter_fill = False
                self.handle_lighter_hedge_result({
                    'side': side.upper(),
                    'filled_size': filled_base_amount,
                    'price': price
                })

    async def setup_clients_websocket(self):
        self.grvt_client.setup_order_update_handler(self.handle_grvt_order_update)
        await self.grvt_client.connect()
        self.logger.info("✅ GRVT Maker WebSocket connected")

        self.lighter_client.setup_order_update_handler(self.handle_lighter_order_update)
        await self.lighter_client.connect()
        self.logger.info("✅ Lighter Taker WebSocket connected")

        await asyncio.sleep(2)

    async def trading_loop(self):
        self.logger.info(f"🚀 Starting GRVT/Lighter hedge bot for {self.ticker}")

        try:
            self.initialize_grvt_client()
            self.initialize_lighter_client()
            await self.get_contract_info()
            self.logger.info(
                f"Contract info loaded - GRVT: {self.grvt_contract_id}, Lighter: {self.lighter_contract_id}")
            await self.setup_clients_websocket()
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        iterations = 0
        while iterations < self.iterations and not self.stop_flag:
            iterations += 1

            if iterations == 1:
                side = self.start_side
            else:
                side = 'buy' if self.current_side == 'sell' else 'sell'
            self.current_side = side

            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}. Net P&L: {self.current_net_pnl:.2f} USD")
            self.logger.info("-----------------------------------------------")

            if abs(self.grvt_position + self.lighter_position) > self.order_quantity * Decimal('0.1'):
                self.logger.critical(f"❌ 倉位差異過大: {self.grvt_position + self.lighter_position}. 停止交易。")
                break

            if self.grvt_position != 0 or self.lighter_position != 0:
                self.logger.warning("倉位未清零，進行外部校準並跳過開倉。")
                await self.fetch_current_exchange_positions()
                await asyncio.sleep(5)
                continue

            self.is_closing = False
            self.order_execution_complete = False
            self.waiting_for_lighter_fill = False
            self.grvt_order_status = None

            try:
                await self.place_grvt_open_order(side, self.order_quantity)

                start_time = time.time()
                while not self.waiting_for_lighter_fill and not self.stop_flag and (
                        time.time() - start_time < self.open_wait_timeout):
                    await asyncio.sleep(0.1)

                if self.waiting_for_lighter_fill and not self.stop_flag:
                    self.logger.info("GRVT filled. Executing Lighter hedge...")
                    hedge_success = await self.place_lighter_market_order(
                        self.current_lighter_side,
                        self.current_lighter_quantity
                    )
                    if not hedge_success and self.grvt_position != 0:
                        self.logger.warning("Hedge failed -> emergency close triggered.")
                        break
                elif not self.stop_flag and self.grvt_order_status not in ['FILLED', 'CANCELED']:
                    self.logger.warning("GRVT Maker order timeout. Canceling and retrying...")
                    if hasattr(self.grvt_client, 'cancel_all_orders'):
                        await self.grvt_client.cancel_all_orders(self.grvt_contract_id)
                    elif hasattr(self.grvt_client, 'cancel_order') and self.grvt_client is not None:
                        if self.current_grvt_order_id:
                            await self.grvt_client.cancel_order(self.current_grvt_order_id)
                    await asyncio.sleep(self.sleep_between_cycles)
                    continue

            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                break

            if self.grvt_position != 0 or self.lighter_position != 0:
                self.open_time = time.time()
                self.pnl_monitor_task = asyncio.create_task(self.calculate_and_monitor_pnl())

                self.logger.info(f"⏳ 雙邊對沖成功建立。等待 {self.holding_time} 秒 (或 PNL 觸發)...")

                while time.time() < self.open_time + self.holding_time and not self.stop_flag and self.open_time != 0:
                    await asyncio.sleep(1)

                if self.pnl_monitor_task:
                    self.pnl_monitor_task.cancel()

                if self.open_time == 0 and not self.stop_flag:
                    self.logger.info("⚠️ PNL 監控觸發緊急平倉，立即執行。")
                else:
                    self.logger.info("✅ 持倉時間已到。執行平倉。")

            if self.grvt_position != 0 or self.lighter_position != 0 and not self.stop_flag:
                await self.close_both_positions()

            start_time = time.time()
            self.is_closing = True
            while (self.grvt_position != 0 or self.lighter_position != 0) and not self.stop_flag and (
                    time.time() - start_time < 30):
                await self.fetch_current_exchange_positions()

                self.logger.info(
                    f"🔄 等待平倉確認... GRVT: {self.grvt_position}, Lighter: {self.lighter_position}")
                await asyncio.sleep(2)

            if self.grvt_position != 0 or self.lighter_position != 0:
                self.logger.critical("🚨 嚴重錯誤：平倉後倉位未清零。手動介入！")
                self.stop_flag = True

            await asyncio.sleep(self.sleep_between_cycles)

    async def run(self):
        self.setup_signal_handlers()
        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()
            await self._disconnect_clients()

    async def _disconnect_clients(self):
        try:
            if self.grvt_client and hasattr(self.grvt_client, "disconnect"):
                await self.grvt_client.disconnect()
        except Exception:
            pass
        try:
            if self.lighter_client and hasattr(self.lighter_client, "disconnect"):
                await self.lighter_client.disconnect()
            elif self.lighter_client and hasattr(self.lighter_client, "close"):
                result = self.lighter_client.close()
                if asyncio.iscoroutine(result):
                    await result
        except Exception:
            pass

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
