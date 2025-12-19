import asyncio
import os
import sys
import logging
from decimal import Decimal
from pathlib import Path

# *************************************************************
# 關鍵：從你的核心交易檔案中導入 HedgeBot 類別
# *************************************************************
try:
    # 假設你的 EdgeX 邏輯檔案名稱為 hedge_mode_edgex.py 且位於 hedge 資料夾內
    # 並且我們在導入時，只需要 Lighter 相關的邏輯。
    # 警告：這個導入會執行 hedge_mode_edgex.py 頂層的所有代碼，
    # 但因為我們不運行 trading_loop，所以不會有副作用。
    sys.path.append(str(Path(__file__).parent))  # 確保可以導入 hedge 資料夾
    from hedge.hedge_mode_edgex import HedgeBot
except ImportError as e:
    print(f"致命錯誤：無法導入 HedgeBot 類別。請確認：")
    print(f"1. 你有 'hedge' 資料夾。")
    print(f"2. 你的核心檔案名稱是 'hedge/hedge_mode_edgex.py'。")
    print(f"導入錯誤訊息: {e}")
    sys.exit(1)

# *************************************************************
# 配置日誌和環境變數
# *************************************************************
# 只需要載入 .env 即可，HedgeBot 內部會調用 os.getenv()
# 假設 .env 文件位於與此腳本相同的目錄中
try:
    from dotenv import load_dotenv

    if not load_dotenv():
        print("警告: 無法載入 .env 檔案。請確認它存在於當前目錄。")
except ImportError:
    print("錯誤: 缺少 python-dotenv 庫。請執行 pip install python-dotenv")
    sys.exit(1)


class LighterTester(HedgeBot):
    """繼承 HedgeBot，但僅用於測試 Lighter 連線。"""

    def __init__(self, ticker):
        # 初始化 Lighter 所需的最小參數，並覆蓋 EdgeX 相關的變數，
        # 避免在 Lighter 測試中觸發不必要的 EdgeX 驗證。

        # 使用一個安全的默認值來滿足 HedgeBot 的 __init__ 簽名
        super().__init__(ticker=ticker, order_quantity=Decimal('0.001'),
                         iterations=1, fill_timeout=5, sleep_time=0)

        # 清除 EdgeX 相關的 client/manager，確保我們只測試 Lighter
        self.edgex_client = None
        self.edgex_ws_manager = None

        # 調整日誌輸出，讓測試輸出更乾淨
        self.logger.handlers.clear()
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
        self.logger.info(f"Tester 初始化完成，準備測試 {ticker}")


async def test_lighter_connection(ticker_to_test: str):
    """執行 Lighter 客戶端初始化和市場配置獲取測試。"""
    tester = LighterTester(ticker=ticker_to_test)

    tester.logger.info("--- Lighter 連線測試開始 ---")

    try:
        # 1. 初始化 Lighter 客戶端 (測試私鑰和身份驗證)
        await tester.initialize_lighter_client()
        tester.logger.info("✅ 步驟 1/2: Lighter 客戶端初始化成功 (憑證有效)")

        # 2. 獲取市場配置 (測試 REST API 連線和 ticker 可用性)
        # 注意: 這裡會檢查你的 ticker (如 BTC) 在 Lighter 上是否存在
        market_id, base_mult, price_mult, tick_size = await tester.get_lighter_market_config()

        tester.logger.info("✅ 步驟 2/2: Lighter REST API 連線成功 (市場配置獲取)")
        tester.logger.info(f"   - Ticker: {ticker_to_test}, Market ID: {market_id}, Tick Size: {tick_size}")
        tester.logger.info("--- Lighter 連線測試：大獲全勝！ ---")
        return 0

    except Exception as e:
        tester.logger.error(f"❌ Lighter 連線測試失敗：{e}")
        tester.logger.error("--- 錯誤排查點：---")
        tester.logger.error("1. 檢查 .env 檔案中 LIGHTER_ACCOUNT_INDEX 和 LIGHTER_API_KEY_INDEX 是否為**十進制**整數。")
        tester.logger.error("2. 檢查 .env 檔案中 API_KEY_PRIVATE_KEY 是否完整且正確。")
        tester.logger.error("3. 確認 Ticker 符號 (BTC) 在 Lighter 交易所上是有效的。")
        tester.logger.error("4. 檢查網路連線是否通暢。")
        return 1

    finally:
        # 確保清理
        await asyncio.sleep(0.1)


if __name__ == "__main__":

    # 確保參數和 Ticker 符號
    if len(sys.argv) < 2:
        print("用法: python test_lighter.py <Ticker>")
        print("範例: python test_lighter.py BTC")
        sys.exit(1)

    ticker = sys.argv[1].upper()

    # 運行異步測試
    sys.exit(asyncio.run(test_lighter_connection(ticker)))