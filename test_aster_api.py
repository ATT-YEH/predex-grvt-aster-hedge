import asyncio
import os
import sys
from decimal import Decimal
import logging
import dotenv  # 確保讀取 .env
import time

# 確保 Python 可以找到 exchanges 模組
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 導入 Aster 客戶端
from exchanges.aster import AsterClient

# 設置基礎配置 (使用小數量和 BTC/USDT)
TEST_TICKER = 'BTC'
TEST_QUANTITY = Decimal('0.001')

# 設置簡單日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ASTER_TEST')


class Config:
    """Simple config structure for Aster client."""

    def __init__(self, ticker, quantity):
        self.ticker = ticker
        self.contract_id = ''
        self.quantity = quantity
        self.tick_size = Decimal(0)
        self.direction = 'sell'  # 隨機方向


async def test_api_calls():
    """執行 Aster API 連接和測試交易流程。"""

    # 載入 .env 檔案中的環境變數
    dotenv.load_dotenv()

    # 1. 初始化客戶端
    logger.info("--- 步驟 1: 初始化 Aster 客戶端 ---")
    try:
        config = Config(TEST_TICKER, TEST_QUANTITY)
        client = AsterClient(config)
        logger.info(f"✅ Aster Client initialized successfully for {TEST_TICKER}.")
    except ValueError as e:
        logger.error(f"❌ 初始化失敗 (憑證/環境變數錯誤): {e}")
        logger.error("🚨 請確保 ASTER_API_KEY 和 ASTER_SECRET_KEY 已在 .env 中設定。")
        return

    # 2. 獲取合約信息
    logger.info("--- 步驟 2: 獲取合約信息 ---")
    try:
        contract_id, tick_size = await client.get_contract_attributes()
        client.config.contract_id = contract_id
        client.config.tick_size = tick_size
        logger.info(f"✅ 合約信息獲取成功: ID={contract_id}, Tick Size={tick_size}.")
    except Exception as e:
        logger.error(f"❌ 合約信息獲取失敗: {e}")
        return

    # 3. 獲取 BBO 價格
    logger.info("--- 步驟 3: 獲取 BBO 價格 ---")
    try:
        best_bid, best_ask = await client.fetch_bbo_prices(contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
            raise ValueError("Invalid BBO prices received.")
        logger.info(f"✅ BBO 價格獲取成功: Bid={best_bid}, Ask={best_ask}.")

    except Exception as e:
        logger.error(f"❌ BBO 獲取失敗: {e}")
        return

    # 4. 放置測試訂單 (Market Order - 確保對沖腿可以立即成交)
    logger.info("--- 步驟 4: 放置測試訂單 (Market Sell Order) ---")
    test_order_id = None
    try:
        # 我們測試 Market Sell Order (Taker 腿的主要功能)
        order_result = await client.place_market_order(
            contract_id=contract_id,
            quantity=TEST_QUANTITY,
            direction='sell'
        )
        test_order_id = order_result.order_id

        # Market Order 應該會立即成交，所以我們檢查狀態
        order_info = await client.get_order_info(order_id=test_order_id)

        if order_info and order_info.status == 'FILLED':
            logger.info(f"✅ 測試訂單放置成功並成交。ID={test_order_id}, Price={order_info.price}.")
        else:
            logger.error(
                f"❌ 測試訂單放置成功，但未立即成交或狀態異常: {order_info.status if order_info else 'Unknown Status'}.")

    except Exception as e:
        logger.error(f"❌ 測試訂單放置失敗: {e}")

    finally:
        # 5. 清理倉位 (如果 Market Order 成功，會建立一個倉位，但由於是小數量，我們暫時忽略手動平倉，專注於 API 呼叫是否成功)
        pass

    logger.info("--- Aster 客戶端測試完成 ---")


if __name__ == "__main__":
    asyncio.run(test_api_calls())
