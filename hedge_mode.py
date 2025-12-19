#!/usr/bin/env python3
"""
Hedge Mode Entry Point - Fixed Version for GRVT/Lighter
"""

import asyncio
import sys
import argparse
from decimal import Decimal
from pathlib import Path
import dotenv


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Hedge Mode Trading Bot Entry Point',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--exchange', type=str, required=True,
                        help='Exchange to use (backpack, extended, apex, grvt, or edgex)')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str, required=True,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--iter', type=int, required=True,
                        help='Number of iterations to run')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')
    parser.add_argument('--sleep', type=int, default=0,
                        help='Sleep time in seconds after each step (default: 0)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")
    parser.add_argument('--max-position', type=Decimal, default=Decimal('0'),
                        help='Maximum position to hold (default: 0)')
    parser.add_argument('--v2', action='store_true',
                        help='Use v2 implementation')

    return parser.parse_args()


def validate_exchange(exchange):
    """Validate that the exchange is supported."""
    supported_exchanges = ['backpack', 'extended', 'apex', 'grvt', 'edgex', 'nado']
    if exchange.lower() not in supported_exchanges:
        print(f"Error: Unsupported exchange '{exchange}'")
        sys.exit(1)


def get_hedge_bot_class(exchange, v2=False):
    """Import and return the appropriate HedgeBot class."""
    try:
        if exchange.lower() == 'backpack':
            from hedge.hedge_mode_bp import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'extended':
            from hedge.hedge_mode_ext import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'apex':
            from hedge.hedge_mode_apex import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'grvt':
            # 🌟 核心修改：不管 v1 還是 v2，都強制使用你跑得通的 grvtaster 版本
            print("Using stable grvtaster implementation...")
            from hedge.hedge_mode_grvtaster import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'edgex':
            from hedge.hedge_mode_edgex import HedgeBot
            return HedgeBot
        elif exchange.lower() == 'nado':
            from hedge.hedge_mode_nado import HedgeBot
            return HedgeBot
        else:
            raise ValueError(f"Unsupported exchange: {exchange}")
    except ImportError as e:
        print(f"Error importing hedge mode implementation: {e}")
        print("Tip: Make sure you are in the correct virtual environment and packages are installed.")
        sys.exit(1)


async def main():
    """Main entry point that creates and runs the appropriate hedge bot."""
    args = parse_arguments()

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not found: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)

    validate_exchange(args.exchange)

    # Get the appropriate HedgeBot class
    HedgeBotClass = get_hedge_bot_class(args.exchange, v2=args.v2)

    print(f"Starting hedge mode for {args.exchange}...")
    print(f"Ticker: {args.ticker}, Size: {args.size}, Iterations: {args.iter}")
    print("-" * 50)

    try:
        # 🌟 參數對齊修改：grvtaster 版本的參數名稱可能與原始 grvt 版本略有不同
        # 根據你之前的 launch_hedge.py，它使用的是 iterations 和 order_quantity
        if args.exchange.lower() == 'grvt':
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                iterations=args.iter
                # 如果噴出 start_side 錯誤，可以加一行: start_side='buy'
            )
        else:
            bot = HedgeBotClass(
                ticker=args.ticker.upper(),
                order_quantity=Decimal(args.size),
                fill_timeout=args.fill_timeout,
                iterations=args.iter,
                sleep_time=args.sleep
            )

        await bot.run()

    except KeyboardInterrupt:
        print("\nHedge mode interrupted by user")
    except Exception as e:
        print(f"Error running hedge mode: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))