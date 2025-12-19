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
from decimal import Decimal
from typing import Optional, Dict, Any, Tuple

# --- 導入新的客戶端 ---
from exchanges.grvt import GrvtClient
from exchanges.lighter import LighterClient

import websockets
from datetime import datetime
import pytz

# 確保可以找到 exchanges 模組 (必須在文件開頭)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- 策略常數 ---
HEDGE_TIMEOUT = 10  # Taker 腿對沖超時時間 (秒)
HOLDING_TIME = 180  # 預期持倉時間 (5 分鐘)
MAX_RISK_USD = Decimal('30')  # 最大可容忍淨浮動虧損 (USD)

# ------------------------------------------------------------
# Config import (align with repo style)
# ------------------------------------------------------------
try:
    from helpers.config import Config  # type: ignore
except Exception:
    class Config:  # fallback
        def __init__(self, d: Dict[str, Any]):
            for k, v in d.items():
                setattr(self, k, v)

HEDGE_TIMEOUT = 10
SLEEP_BETWEEN_CYCLES = 0.2


def _normalize_order_result(res: Any) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Normalize different return types into:
      (ok, order_id, error_message)

    Supports:
      - OrderResult: has .success, .order_id, .error_message
      - OrderInfo: has .order_id, .status
      - dict: keys may be success/order_id/error_message/id
    """
    if res is None:
        return False, None, "order result is None"

    # dict style
    if isinstance(res, dict):
        ok = bool(res.get("success", True))  # many dict results don't have success; treat as ok unless explicit False
        oid = res.get("order_id") or res.get("id") or res.get("client_order_id")
        err = res.get("error_message") or res.get("error") or res.get("message")
        if ok is False and not err:
            err = "order failed (dict)"
        return ok, str(oid) if oid is not None else None, err

    # OrderResult style
    if hasattr(res, "success"):
        ok = bool(getattr(res, "success"))
        oid = getattr(res, "order_id", None)
        err = getattr(res, "error_message", None)
        return ok, str(oid) if oid is not None else None, err

    # OrderInfo style
    if hasattr(res, "order_id"):
        oid = getattr(res, "order_id", None)
        # If it returned an OrderInfo object, assume placement succeeded (status might be OPEN/PENDING)
        return True, str(oid) if oid is not None else None, None

    # unknown object
    return False, None, f"unknown order result type: {type(res)}"


class HedgeBot:
    """
    GRVT (maker post-only limit) -> Lighter (hedge market/IOC)
    Trigger hedge ONLY on GRVT WS fill event.
    """

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int = 10,
        iterations: int = 5,
        start_side: str = "buy",
    ):
        self.ticker = ticker
        self.order_quantity = Decimal(order_quantity)
        self.fill_timeout = int(fill_timeout)
        self.iterations = int(iterations)
        self.start_side = start_side.lower()

        self.logger = logging.getLogger("HedgeBot-GRVT-Lighter")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            ch.setFormatter(fmt)
            self.logger.addHandler(ch)

        self.grvt_client: Optional[GrvtClient] = None
        self.lighter_client: Optional[LighterClient] = None

        self.grvt_contract_id: Optional[str] = None
        self.grvt_tick_size: Optional[Decimal] = None
        self.lighter_contract_id: Optional[str] = None
        self.lighter_tick_size: Optional[Decimal] = None

        # maker state
        self.waiting_for_grvt_fill = False
        self.current_grvt_order_id: Optional[str] = None
        self.last_grvt_fill_side: str = ""
        self.last_grvt_fill_size: Decimal = Decimal("0")
        self.last_grvt_fill_price: Decimal = Decimal("0")

        # hedge state
        self.waiting_for_lighter_fill = False
        self.current_lighter_client_order_id: Optional[str] = None
        self.last_lighter_fill_side: str = ""
        self.last_lighter_fill_size: Decimal = Decimal("0")
        self.last_lighter_fill_price: Decimal = Decimal("0")

        self._shutdown = False

    async def initialize_clients(self):
        if self.grvt_client is None:
            grvt_cfg = Config({
                "ticker": self.ticker,
                "contract_id": "",
                "quantity": self.order_quantity,
                "tick_size": Decimal("0.01"),
                "close_order_side": "sell",
            })
            self.grvt_client = GrvtClient(grvt_cfg)

        if self.lighter_client is None:
            lighter_cfg = Config({
                "ticker": self.ticker,
                "contract_id": "",
                "quantity": self.order_quantity,
                "tick_size": Decimal("0.01"),
                "close_order_side": "sell",
            })
            self.lighter_client = LighterClient(lighter_cfg)

        # WS fill handlers
        self.grvt_client.setup_order_update_handler(self._on_grvt_order_updates)
        self.lighter_client.setup_order_update_handler(self._on_lighter_order_updates)

        self.logger.info("Connecting GRVT...")
        await self.grvt_client.connect()

        self.logger.info("Connecting Lighter...")
        await self.lighter_client.connect()

        await self._init_contracts()

    async def _init_contracts(self):
        self.grvt_contract_id, self.grvt_tick_size = await self.grvt_client.get_contract_attributes()
        self.lighter_contract_id, self.lighter_tick_size = await self.lighter_client.get_contract_attributes()

        self.logger.info(f"GRVT contract_id={self.grvt_contract_id} tick={self.grvt_tick_size}")
        self.logger.info(f"Lighter contract_id={self.lighter_contract_id} tick={self.lighter_tick_size}")

    # ---------------- WS Handlers ----------------
    def _on_grvt_order_updates(self, orders: list):
        if not orders:
            return
        for od in orders:
            try:
                status = str(od.get("status", "")).upper()
                side = str(od.get("side", "")).lower()
                order_id = str(od.get("order_id", od.get("id", "")))
                filled = Decimal(str(od.get("filled_size", od.get("filled", 0)) or 0))
                price = Decimal(str(od.get("price", 0) or 0))

                if not self.waiting_for_grvt_fill:
                    continue
                if self.current_grvt_order_id and order_id != str(self.current_grvt_order_id):
                    continue

                if status == "FILLED" and filled > 0:
                    self.last_grvt_fill_side = side
                    self.last_grvt_fill_size = filled
                    self.last_grvt_fill_price = price
                    self.waiting_for_grvt_fill = False
                    self.logger.info(f"✅ GRVT FILLED: {side} {filled} @ {price} (order_id={order_id})")
            except Exception as e:
                self.logger.error(f"GRVT WS handler error: {e}")

    def _on_lighter_order_updates(self, orders: list):
        if not orders:
            return
        for od in orders:
            try:
                status = str(od.get("status", "")).upper()
                is_ask = bool(od.get("is_ask", False))
                side = "sell" if is_ask else "buy"

                client_order_index = od.get("client_order_index", None)
                filled_base_amount = Decimal(str(od.get("filled_base_amount", 0) or 0))
                price = Decimal(str(od.get("price", 0) or 0))

                if not self.waiting_for_lighter_fill:
                    continue
                if self.current_lighter_client_order_id is not None:
                    if str(client_order_index) != str(self.current_lighter_client_order_id):
                        continue

                if status == "OPEN" and filled_base_amount > 0:
                    status = "PARTIALLY_FILLED"

                if status == "FILLED" and filled_base_amount > 0:
                    self.last_lighter_fill_side = side
                    self.last_lighter_fill_size = filled_base_amount
                    self.last_lighter_fill_price = price
                    self.waiting_for_lighter_fill = False
                    self.logger.info(
                        f"✅ Lighter FILLED: {side} {filled_base_amount} @ {price} (client_order_index={client_order_index})"
                    )
            except Exception as e:
                self.logger.error(f"Lighter WS handler error: {e}")

    # ---------------- Orders ----------------
    async def place_grvt_maker_order(self, side: str, quantity: Decimal) -> bool:
        assert self.grvt_client is not None

        side = side.lower()
        price = await self.grvt_client.get_order_price(side)
        price = self.grvt_client.round_to_tick(price) if hasattr(self.grvt_client, "round_to_tick") else price

        # reset fill cache
        self.last_grvt_fill_side = ""
        self.last_grvt_fill_size = Decimal("0")
        self.last_grvt_fill_price = Decimal("0")

        self.logger.info(f"🧩 Placing GRVT maker (post-only): {side} {quantity} @ {price}")

        res = await self.grvt_client.place_post_only_order(self.grvt_contract_id, quantity, price, side)

        ok, oid, err = _normalize_order_result(res)
        if not ok:
            self.logger.warning(f"❌ GRVT maker order failed: {err}")
            return False

        if oid is None:
            # even if order placement succeeded, we need an id to track fill
            self.logger.warning("❌ GRVT maker order returned no order_id; cannot track fills.")
            return False

        self.current_grvt_order_id = str(oid)
        self.waiting_for_grvt_fill = True
        self.logger.info(f"🧾 GRVT maker order placed. order_id={self.current_grvt_order_id}")
        return True

    async def wait_grvt_fill_or_timeout(self) -> bool:
        assert self.grvt_client is not None

        start = time.time()
        while self.waiting_for_grvt_fill and (time.time() - start) < self.fill_timeout and not self._shutdown:
            await asyncio.sleep(0.05)

        if not self.waiting_for_grvt_fill:
            return True

        self.logger.warning(f"⏰ GRVT maker fill timeout ({self.fill_timeout}s). Canceling...")
        if self.current_grvt_order_id:
            try:
                await self.grvt_client.cancel_order(self.current_grvt_order_id)
            except Exception as e:
                self.logger.error(f"GRVT cancel error: {e}")

        self.waiting_for_grvt_fill = False
        return False

    async def place_lighter_market_order(self, side: str, quantity: Decimal) -> bool:
        assert self.lighter_client is not None

        side = side.lower()

        self.last_lighter_fill_side = ""
        self.last_lighter_fill_size = Decimal("0")
        self.last_lighter_fill_price = Decimal("0")
        self.current_lighter_client_order_id = None

        self.logger.info(f"⚖️ Hedging on Lighter (MARKET/IOC): {side} {quantity}")

        res = await self.lighter_client.place_market_order(self.lighter_contract_id, quantity, side)
        ok, oid, err = _normalize_order_result(res)
        if not ok:
            self.logger.error(f"❌ Lighter hedge order failed: {err}")
            return False

        if oid is None:
            self.logger.error("❌ Lighter hedge returned no order_id/client_order_index; cannot track fills.")
            return False

        self.current_lighter_client_order_id = str(oid)
        self.waiting_for_lighter_fill = True
        self.logger.info(f"🧾 Lighter hedge sent. client_order_index={self.current_lighter_client_order_id}")
        return True

    async def wait_lighter_fill_or_timeout(self) -> bool:
        start = time.time()
        while self.waiting_for_lighter_fill and (time.time() - start) < HEDGE_TIMEOUT and not self._shutdown:
            await asyncio.sleep(0.05)

        if not self.waiting_for_lighter_fill:
            return True

        self.logger.error(f"⏰ Lighter hedge timeout ({HEDGE_TIMEOUT}s).")
        self.waiting_for_lighter_fill = False
        return False

    # ---------------- Loop ----------------
    async def trading_loop(self):
        side = self.start_side

        for i in range(self.iterations):
            if self._shutdown:
                break

            self.logger.info(f"================= Cycle {i+1}/{self.iterations} (start_side={side}) =================")

            ok = await self.place_grvt_maker_order(side, self.order_quantity)
            if not ok:
                await asyncio.sleep(SLEEP_BETWEEN_CYCLES)
                side = "sell" if side == "buy" else "buy"
                continue

            filled = await self.wait_grvt_fill_or_timeout()
            if not filled:
                await asyncio.sleep(SLEEP_BETWEEN_CYCLES)
                side = "sell" if side == "buy" else "buy"
                continue

            hedge_side = "sell" if self.last_grvt_fill_side == "buy" else "buy"
            hedge_qty = self.last_grvt_fill_size

            ok = await self.place_lighter_market_order(hedge_side, hedge_qty)
            if not ok:
                self.logger.error("❌ Hedge send failed. (TODO: emergency flatten on GRVT)")
                await asyncio.sleep(SLEEP_BETWEEN_CYCLES)
                side = "sell" if side == "buy" else "buy"
                continue

            hedge_filled = await self.wait_lighter_fill_or_timeout()
            if not hedge_filled:
                self.logger.error("❌ Hedge not confirmed FILLED by WS. (TODO: emergency handling)")
                await asyncio.sleep(SLEEP_BETWEEN_CYCLES)
                side = "sell" if side == "buy" else "buy"
                continue

            await asyncio.sleep(SLEEP_BETWEEN_CYCLES)
            side = "sell" if side == "buy" else "buy"

    async def run(self):
        self.logger.info(f"🚀 HedgeBot Starting: {self.ticker} | Size: {self.order_quantity} | Iter: {self.iterations}")
        _install_signal_handlers(self)

        try:
            await self.initialize_clients()
            await self.trading_loop()
            self.logger.info("🏁 Completed.")
        finally:
            # clean disconnect to avoid aiohttp session leaks
            try:
                if self.grvt_client and hasattr(self.grvt_client, "disconnect"):
                    await self.grvt_client.disconnect()
            except Exception:
                pass
            try:
                if self.lighter_client and hasattr(self.lighter_client, "disconnect"):
                    await self.lighter_client.disconnect()
            except Exception:
                pass

    def request_shutdown(self):
        self._shutdown = True
        self.logger.warning("Shutdown requested...")


def _install_signal_handlers(bot: HedgeBot):
    def handler(signum, frame):
        bot.request_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
