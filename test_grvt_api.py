import asyncio
import os
import sys
from decimal import Decimal
import logging
import dotenv  # 新增 dotenv 導入

# 確保 Python 可以找到 exchanges 模組
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 導入 GRVT 客戶端
from exchanges.grvt import GrvtClient

# 設置基礎配置 (使用小數量和 BTC/USDT)
TEST_TICKER = 'BTC'
TEST_QUANTITY = Decimal('0.001')  # 最小測試數量
TEST_FILL_TIMEOUT = 10  # 秒

# 設置簡單日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GRVT_TEST')


class Config:
    """Simple config structure for GRVT client."""

    def __init__(self, ticker, quantity):
        self.ticker = ticker
        self.contract_id = ''  # 會在客戶端內部設置
        self.quantity = quantity
        self.tick_size = Decimal(0)
        self.direction = 'buy'  # 隨機方向


async def test_api_calls():
    """執行 GRVT API 連接和測試交易流程。"""

    # 🌟 修正點：強制載入 .env 檔案中的環境變數
    # 假設 .env 檔案在程式碼執行的目錄下 (~/predex/)
    dotenv.load_dotenv()

    # 1. 初始化客戶端
    logger.info("--- 步驟 1: 初始化客戶端 ---")
    try:
        config = Config(TEST_TICKER, TEST_QUANTITY)
        # 修正點：直接傳遞 config 物件，而不是字典
        client = GrvtClient(config)
        logger.info(f"✅ GRVT Client initialized successfully for {TEST_TICKER}.")
    except ValueError as e:
        logger.error(f"❌ 初始化失敗 (憑證/環境變數錯誤): {e}")
        return

    # 2. 獲取合約信息 (驗證 API 憑證是否有效)
    logger.info("--- 步驟 2: 獲取合約信息 (驗證 API 憑證) ---")
    try:
        contract_id, tick_size = await client.get_contract_attributes()
        client.config.contract_id = contract_id
        client.config.tick_size = tick_size
        logger.info(f"✅ 合約信息獲取成功: ID={contract_id}, Tick Size={tick_size}.")
    except Exception as e:
        logger.error(f"❌ 合約信息獲取失敗 (API 權限/連接問題): {e}")
        logger.error("🚨 錯誤碼 403 通常發生在這裡，請檢查您的憑證權限。")
        return

    # 3. 獲取 BBO 價格
    logger.info("--- 步驟 3: 獲取 BBO 價格 ---")
    try:
        best_bid, best_ask = await client.fetch_bbo_prices(contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
            raise ValueError("Invalid BBO prices received.")
        logger.info(f"✅ BBO 價格獲取成功: Bid={best_bid}, Ask={best_ask}.")

        # 計算測試訂單的價格 (Maker 價)
        test_price = best_ask - tick_size  # 買單掛在 Ask 價下方一檔
        logger.info(f"計算測試訂單價格: {test_price}")

    except Exception as e:
        logger.error(f"❌ BBO 獲取失敗: {e}")
        return

    # 4. 放置測試訂單 (Maker Order)
    logger.info("--- 步驟 4: 放置測試訂單 (Maker) ---")
    test_order_id = None
    try:
        # 使用 place_post_only_order 測試下單功能
        order_result = await client.place_post_only_order(
            contract_id=contract_id,
            quantity=TEST_QUANTITY,
            price=test_price,
            side='buy'
        )
        test_order_id = order_result.order_id
        logger.info(f"✅ 測試訂單放置成功。ID={test_order_id}, Status={order_result.status}.")

    except Exception as e:
        logger.error(f"❌ 測試訂單放置失敗: {e}")

    finally:
        # 5. 清理訂單
        if test_order_id:
            logger.info("--- 步驟 5: 清理訂單 ---")
            try:
                # 嘗試取消訂單
                cancel_result = await client.cancel_order(test_order_id)
                if cancel_result.success:
                    logger.info(f"✅ 測試訂單 ID={test_order_id} 取消成功。")
                else:
                    logger.warning(f"⚠️ 訂單 ID={test_order_id} 取消失敗。可能已成交或被拒絕。")
            except Exception as e:
                logger.error(f"❌ 取消操作異常: {e}")

    logger.info("--- GRVT 客戶端測試完成 ---")


if __name__ == "__main__":
    asyncio.run(test_api_calls())
