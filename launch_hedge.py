import asyncio
import sys
import argparse
from decimal import Decimal
import os
import dotenv
from pathlib import Path

# 確保 Python 可以找到 exchanges 模組
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 直接從正確的路徑導入您的核心 Bot
from hedge.hedge_mode_grvtaster import HedgeBot


def parse_arguments():
    """解析命令行參數，允許用戶設置 ticker, size, 和 iterations。"""
    parser = argparse.ArgumentParser(description='GRVT/Aster Hedge Mode Launcher')
    parser.add_argument('--ticker', type=str, default='BTC', help='交易對符號 (預設: BTC)')
    parser.add_argument('--size', type=Decimal, default=Decimal('0.001'), help='每筆訂單數量 (預設: 0.001)')
    parser.add_argument('--iter', type=int, default=5, help='交易循環次數 (預設: 5)')
    parser.add_argument('--fill-timeout', type=int, default=10, help='Maker訂單成交超時 (預設: 10秒)')
    # 🌟 新增參數: 決定第一個循環的起始方向
    parser.add_argument('--start-side', type=str, choices=['buy', 'sell'], default='buy',
                        help='第一個循環的開倉方向 (預設: buy)')
    return parser.parse_args()


async def start_bot():
    """初始化並運行 HedgeBot，直接使用 run() 方法。"""
    args = parse_arguments()
    dotenv.load_dotenv('.env')

    print(f"Starting GRVT/Aster Hedge Mode: {args.ticker} Size: {args.size}, Start Side: {args.start_side}")
    print("-" * 50)

    bot = HedgeBot(
        ticker=args.ticker.upper(),
        order_quantity=args.size,
        fill_timeout=args.fill_timeout,
        iterations=args.iter,
        # 傳遞新的起始方向參數
        start_side=args.start_side
    )

    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except Exception as e:
        print(f"An unexpected error occurred: {e}")