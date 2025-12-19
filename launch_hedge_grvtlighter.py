import asyncio
import sys
import argparse
from decimal import Decimal
import os
import traceback
import dotenv
from pathlib import Path

# ---- Project path setup ----
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import bot
from hedge.hedge_mode_grvtlighter import HedgeBot


def parse_arguments():
    """解析命令行參數：ticker / size / iter / fill-timeout / start-side / env"""
    parser = argparse.ArgumentParser(description="GRVT/Lighter Hedge Mode Launcher")
    parser.add_argument("--ticker", type=str, default="BTC", help="交易對符號 (預設: BTC)")
    parser.add_argument("--size", type=Decimal, default=Decimal("0.001"), help="每筆訂單數量 (預設: 0.001)")
    parser.add_argument("--iter", type=int, default=5, help="交易循環次數 (預設: 5)")
    parser.add_argument("--fill-timeout", type=int, default=10, help="Maker 訂單成交超時 (預設: 10 秒)")
    parser.add_argument(
        "--start-side",
        type=str,
        choices=["buy", "sell"],
        default="buy",
        help="第一個循環的開倉方向 (預設: buy)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default=str(PROJECT_ROOT / ".env"),
        help="指定 .env 路徑 (預設: 專案同目錄 .env)"
    )
    return parser.parse_args()


def load_env(env_path: str) -> None:
    """Load .env from given path (best-effort)."""
    p = Path(env_path).expanduser().resolve()
    if p.exists():
        dotenv.load_dotenv(str(p))
    else:
        # 仍允許使用系統環境變數
        print(f"[WARN] .env not found at: {p} (will rely on existing environment variables)")


def validate_env() -> None:
    """
    在啟動前檢查關鍵 env，避免跑一半才爆。
    GRVT/Lighter 需要哪些變數，依你專案而定；這裡先檢查 Lighter 端必要 3 個。
    """
    required = ["API_KEY_PRIVATE_KEY", "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            f"Missing required env vars for Lighter: {missing}\n"
            f"Please set them in .env or system environment."
        )


async def start_bot():
    args = parse_arguments()
    load_env(args.env)

    print(
        f"Starting GRVT/Lighter Hedge Mode: {args.ticker.upper()} | "
        f"Size: {args.size} | Iter: {args.iter} | FillTimeout: {args.fill_timeout}s | "
        f"StartSide: {args.start_side}"
    )
    print("-" * 60)

    # 檢查必要 env
    validate_env()

    bot = HedgeBot(
        ticker=args.ticker.upper(),
        order_quantity=args.size,
        fill_timeout=args.fill_timeout,
        iterations=args.iter,
        start_side=args.start_side
    )

    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        print("---- traceback ----")
        print(traceback.format_exc())
        sys.exit(1)
