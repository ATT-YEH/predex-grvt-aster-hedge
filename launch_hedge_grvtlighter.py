import asyncio
import sys
import argparse
from decimal import Decimal
import os
import traceback
import dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hedge.hedge_mode_grvtlighter import HedgeBot


def parse_arguments():
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
    parser.add_argument("--holding-time", type=int, default=180, help="持倉時間 (秒) (預設: 180)")
    parser.add_argument("--hedge-timeout", type=int, default=10, help="對沖成交超時 (秒) (預設: 10)")
    parser.add_argument(
        "--max-risk-usd",
        type=Decimal,
        default=Decimal("30"),
        help="最大可容忍淨浮動虧損 (USD) (預設: 30)"
    )
    parser.add_argument(
        "--sleep-between-cycles",
        type=float,
        default=10.0,
        help="每個循環間的休息秒數 (預設: 10)"
    )
    parser.add_argument(
        "--open-wait-timeout",
        type=int,
        default=None,
        help="等待 GRVT 開倉成交的超時 (秒)。未指定時使用 fill-timeout。"
    )
    parser.add_argument(
        "--grvt-force-market",
        action="store_true",
        help="強制 GRVT 開倉使用 taker/market-like 以保證成交 (debug 用)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default=str(PROJECT_ROOT / ".env"),
        help="指定 .env 路徑 (預設: 專案同目錄 .env)"
    )
    return parser.parse_args()


def load_env(env_path: str) -> None:
    p = Path(env_path).expanduser().resolve()
    if p.exists():
        dotenv.load_dotenv(str(p))
    else:
        print(f"[WARN] .env not found at: {p} (will rely on existing environment variables)")


def validate_env() -> None:
    required = ["API_KEY_PRIVATE_KEY", "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            f"Missing required env vars for Lighter: {missing}\n"
            f"Please set them in .env or system environment."
        )


def print_config(args) -> None:
    config = {
        "ticker": args.ticker.upper(),
        "size": str(args.size),
        "iter": args.iter,
        "fill_timeout": args.fill_timeout,
        "start_side": args.start_side,
        "holding_time": args.holding_time,
        "hedge_timeout": args.hedge_timeout,
        "max_risk_usd": str(args.max_risk_usd),
        "sleep_between_cycles": args.sleep_between_cycles,
        "open_wait_timeout": args.open_wait_timeout if args.open_wait_timeout is not None else args.fill_timeout,
        "grvt_force_market": args.grvt_force_market,
        "env": args.env,
    }

    print("Starting GRVT/Lighter Hedge Mode with config:")
    for key, value in config.items():
        print(f"  - {key}: {value}")
    print("-" * 60)


async def start_bot():
    args = parse_arguments()
    load_env(args.env)

    validate_env()
    print_config(args)

    bot = HedgeBot(
        ticker=args.ticker.upper(),
        order_quantity=args.size,
        fill_timeout=args.fill_timeout,
        iterations=args.iter,
        start_side=args.start_side,
        holding_time=args.holding_time,
        hedge_timeout=args.hedge_timeout,
        max_risk_usd=args.max_risk_usd,
        sleep_between_cycles=args.sleep_between_cycles,
        open_wait_timeout=args.open_wait_timeout,
        grvt_force_market=args.grvt_force_market,
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
