#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hyperliquid Maker 刷量 + Lighter 對沖（自動化）

- 在 Hyperliquid 以 ALO（post-only）掛雙邊被動單；自動跟價、TTL 到期改價
- 透過 userFills 偵測成交，立即到 Lighter 反向對沖（市價或近市價 IOC）
- CSV 全量記錄（quotes/orders/fills/hedges）

依賴：
  pip install hyperliquid-python-sdk lighter-sdk websockets httpx python-dotenv pydantic
"""

import os, sys, csv, time, math, asyncio, signal, random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple

from dotenv import load_dotenv

# --- Hyperliquid SDK ---
from hyperliquid.info import Info
from hyperliquid.utils import constants as hl_const
from hyperliquid.exchange import Exchange
from hyperliquid.ws import Websocket as HlWs  # SDK 內建 WS client（命名可能為 Websocket）

# --- Lighter SDK（官方 PyPI: lighter-sdk）---
try:
    # SDK 版本仍在演進，以下以常見命名示意；若版本不同，請依 pip show lighter-sdk 的 README 為準
    from lighter_sdk import PerpsApi, ApiConfig, EvmSigner  # type: ignore
except Exception:
    PerpsApi = None
    ApiConfig = None
    EvmSigner = None

# ---------- 讀取設定 ----------
load_dotenv()

def env_str(k, d=""):
    return (os.getenv(k) or d).strip()

def env_float(k, d):
    v = os.getenv(k)
    return float(v) if v is not None and v != "" else d

def env_int(k, d):
    v = os.getenv(k)
    return int(v) if v is not None and v != "" else d

@dataclass
class Config:
    log_dir: str = env_str("LOG_DIR", "logs")

    # HL
    hl_network: str = env_str("HL_NETWORK", "mainnet")
    hl_addr: str = env_str("HL_ACCOUNT_ADDRESS", "")
    hl_secret: str = env_str("HL_SECRET_KEY", "")
    hl_symbol: str = env_str("HL_SYMBOL", "ETH")
    side_mode: str = env_str("HL_SIDE_MODE", "both")  # both/buy/sell
    order_usd: float = env_float("HL_ORDER_USD", 50.0)
    max_on_side: int = env_int("HL_MAX_ORDERS_PER_SIDE", 1)
    ttl_s: float = env_float("HL_TTL_S", 10.0)
    cooldown_s: float = env_float("HL_COOLDOWN_S", 1.5)
    bid_offset_ticks: int = env_int("HL_BID_OFFSET_TICKS", 0)
    ask_offset_ticks: int = env_int("HL_ASK_OFFSET_TICKS", 0)
    deadman_s: int = env_int("HL_DEADMAN_S", 30)

    # Lighter
    lighter_enabled: bool = env_str("LIGHTER_ENABLED", "false").lower() == "true"
    lighter_symbol: str = env_str("LIGHTER_SYMBOL", "ETH-PERP")
    lighter_api_key: str = env_str("LIGHTER_API_KEY", "")
    lighter_api_secret: str = env_str("LIGHTER_API_SECRET", "")
    lighter_evm_priv: str = env_str("LIGHTER_EVM_PRIVATE_KEY", "")
    lighter_env: str = env_str("LIGHTER_ENV", "mainnet")

CFG = Config()
os.makedirs(CFG.log_dir, exist_ok=True)

# ---------- CSV 初始化 ----------
Q_CSV = os.path.join(CFG.log_dir, "quotes.csv")
O_CSV = os.path.join(CFG.log_dir, "orders.csv")
F_CSV = os.path.join(CFG.log_dir, "fills.csv")
H_CSV = os.path.join(CFG.log_dir, "hedges.csv")

def _init_csv(path: str, header: list[str]):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)

_init_csv(Q_CSV, ["ts","venue","symbol","bid","ask","mid"])
_init_csv(O_CSV, ["ts","venue","side","px","sz","oid","cloid","note"])
_init_csv(F_CSV, ["ts","venue","side","px","sz","notional","fee","note"])
_init_csv(H_CSV, ["ts","hedgeVenue","srcFillVenue","side","px","sz","notional","status","note"])

now = lambda: datetime.now(timezone.utc).isoformat()

# ---------- Hyperliquid 連線 ----------
if CFG.hl_network.lower() == "testnet":
    API_URL = hl_const.TESTNET_API_URL
    WS_URL  = hl_const.TESTNET_WS_URL
else:
    API_URL = hl_const.MAINNET_API_URL
    WS_URL  = hl_const.MAINNET_WS_URL

if not CFG.hl_addr or not CFG.hl_secret:
    print("[CONFIG] 請在 .env 設定 HL_ACCOUNT_ADDRESS 與 HL_SECRET_KEY")
    sys.exit(1)

info = Info(API_URL, skip_ws=True)
exch = Exchange(CFG.hl_secret, CFG.hl_addr, API_URL)

# 把字串 symbol 轉成 asset id（Perps 用 meta.universe 的 index）
meta = info.meta()
universe = [c["name"] for c in meta["universe"]]  # e.g. ["BTC","ETH",...]
try:
    asset = universe.index(CFG.hl_symbol)
except ValueError:
    print(f"[CONFIG] HL_SYMBOL={CFG.hl_symbol} 不在 meta.universe 裡：{universe[:10]} ...")
    sys.exit(1)

# 取得 tick/lot
tick_info = info.meta()["assetCtxs"][asset]["szDecimals"]  # lot size decimals（下方仍以簡化法處理）
tick_size = float(info.meta()["perpMeta"]["book"]["tickSize"])  # 若 SDK 有現成欄位可直接取
lot_decimals = tick_info if isinstance(tick_info, int) else 3  # 後備

def round_to_tick(px: float) -> float:
    return round(px / tick_size) * tick_size

def round_size(sz: float) -> str:
    # HL 需要字串格式且遵守 lot，小數位依 coin 而定；簡化處理：
    return f"{sz:.{max(0, lot_decimals)}f}"

# Dead-man switch
def schedule_cancel_all(seconds: int):
    try:
        exch.schedule_cancel(seconds*1000)
    except Exception as e:
        print("[WARN] schedule_cancel 失敗：", e)

schedule_cancel_all(CFG.deadman_s)

# ---------- Lighter 連線（可關閉） ----------
class LighterClient:
    def __init__(self):
        self.enabled = CFG.lighter_enabled and PerpsApi is not None
        self.api = None
        if self.enabled:
            try:
                signer = None
                if CFG.lighter_evm_priv:
                    signer = EvmSigner(CFG.lighter_evm_priv)  # 依 SDK 版本可能不同
                cfg = ApiConfig(env=CFG.lighter_env, api_key=CFG.lighter_api_key, api_secret=CFG.lighter_api_secret, signer=signer)
                self.api = PerpsApi(cfg)
                print("[LIGHTER] 已初始化 PerpsApi")
            except Exception as e:
                print("[LIGHTER] 初始化失敗，切換為紀錄模式：", e)
                self.enabled = False

    def hedge_market(self, symbol: str, is_buy: bool, notional_usd: float) -> Tuple[str, str]:
        """
        嘗試以市價/IOC 反向對沖（簡化：用名目換算合約數量由 SDK 內部處理）
        回傳：(status, note)
        """
        side = "buy" if is_buy else "sell"
        if not self.enabled or self.api is None:
            return ("disabled", "lighter disabled or sdk missing")

        try:
            # 具體下單介面依 SDK 版本而異，下列為常見樣式（請視 SDK README 調整）
            # resp = self.api.create_order(symbol=symbol, side=side, type="market", sizeUsd=str(int(notional_usd*1e6)))
            resp = self.api.create_market_order(symbol=symbol, side=side, notional_usd=notional_usd)
            return ("ok", f"id={getattr(resp,'orderId', 'n/a')}")
        except Exception as e:
            return ("error", str(e))

lighter = LighterClient()

# ---------- 場內狀態 ----------
open_oids = {"buy": [], "sell": []}
last_refresh_ts = 0.0

async def fetch_bbo() -> Tuple[float, float]:
    ob = info.best_bid_ask(CFG.hl_symbol)
    bid = float(ob["bestBid"])
    ask = float(ob["bestAsk"])
    mid = 0.5*(bid+ask)
    with open(Q_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([now(), "HL", CFG.hl_symbol, f"{bid:.8f}", f"{ask:.8f}", f"{mid:.8f}"])
    return bid, ask

def usd_to_size(px: float, usd: float) -> float:
    # HL Perp：名目約為 px * size（忽略細節）
    return max(usd / max(px, 1e-9), 0.0)

def can_side(side: str) -> bool:
    if CFG.side_mode == "both": return True
    return CFG.side_mode == side

def note_write(path: str, row: list):
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def place_alo(side: str, px: float, sz: float):
    tif = {"limit": {"tif": "Alo"}}
    order = {
        "a": asset,
        "b": (side == "buy"),
        "p": f"{round_to_tick(px):.6f}",
        "s": round_size(sz),
        "r": False,
        "t": tif,
        "c": f"cl-{int(time.time()*1000)}-{random.randint(100,999)}"
    }
    try:
        resp = exch.order(order, grouping="na")
        # 依 SDK 版本，回傳格式略有差異；下方做穩健解析
        oid = None
        if isinstance(resp, dict):
            data = resp.get("response", {}).get("data", {})
            statuses = data.get("statuses") or []
            if statuses:
                st = statuses[0]
                if "resting" in st:
                    oid = st["resting"]["oid"]
        open_oids[side].append(oid)
        note_write(O_CSV, [now(), "HL", side, order["p"], order["s"], oid, order["c"], "alo"])
        return oid
    except Exception as e:
        note_write(O_CSV, [now(), "HL", side, f"{px:.6f}", round_size(sz), "", "", f"err:{e}"])
        return None

def cancel_all_side(side: str):
    # 逐一取消；也可用 dead-man switch 做保底
    oids = [o for o in open_oids[side] if o]
    if not oids: return
    cancels = [{"a": asset, "o": int(oid)} for oid in oids]
    try:
        exch.cancel(cancels)
    except Exception as e:
        print("[WARN] cancel 失敗：", e)
    open_oids[side].clear()

async def maker_loop():
    global last_refresh_ts
    while True:
        try:
            bid, ask = await asyncio.get_event_loop().run_in_executor(None, fetch_bbo)
        except Exception as e:
            print("[WARN] 取 BBO 失敗：", e)
            await asyncio.sleep(0.5)
            continue

        # 計算兩邊掛價
        if can_side("buy"):
            px_b = max(0.0, bid + CFG.bid_offset_ticks * tick_size)
            sz_b = usd_to_size(px_b, CFG.order_usd)
        if can_side("sell"):
            px_a = max(0.0, ask - CFG.ask_offset_ticks * tick_size)
            sz_a = usd_to_size(px_a, CFG.order_usd)

        # TTL 到就全部改價
        now_ts = time.time()
        need_refresh = (now_ts - last_refresh_ts) >= CFG.ttl_s

        for side in ("buy","sell"):
            if not can_side(side):
                cancel_all_side(side)
                continue
            # 控制最大掛單數
            if len(open_oids[side]) >= CFG.max_on_side and not need_refresh:
                continue
            # 刷新策略：達 TTL 先全撤再重掛
            if need_refresh:
                cancel_all_side(side)
                await asyncio.sleep(CFG.cooldown_s)

        # 重新掛最新價
        if can_side("buy") and (len(open_oids["buy"]) < CFG.max_on_side):
            place_alo("buy", px_b, sz_b)
            await asyncio.sleep(CFG.cooldown_s)
        if can_side("sell") and (len(open_oids["sell"]) < CFG.max_on_side):
            place_alo("sell", px_a, sz_a)
            await asyncio.sleep(CFG.cooldown_s)

        if need_refresh:
            schedule_cancel_all(CFG.deadman_s)
            last_refresh_ts = now_ts

        await asyncio.sleep(0.3)  # 主迴圈節流

# ---- 成交訂閱（userFills）並觸發對沖 ----
async def fills_loop():
    # HL 官方 WS：訂閱 userFills / orderUpdates
    ws = HlWs(WS_URL)
    try:
        # 登入（若 SDK 要求先發 auth，或用 Info/Exchange 提供的 token；這裡以 SDK 內部處理為前提）
        # 訂閱 userFills
        ws.subscribe({"type": "userFills", "user": CFG.hl_addr})
        while True:
            msg = ws.recv()  # 同步接口；若 SDK 提供 async 版本，請改 await
            if not msg:
                await asyncio.sleep(0.1)
                continue
            if isinstance(msg, dict) and msg.get("channel") == "userFills":
                fills = msg.get("data", {}).get("fills") or []
                for f in fills:
                    try:
                        side = "buy" if f.get("isTaker") and f.get("isBuy") else ("buy" if f.get("isBuy") else "sell")
                        px = float(f.get("px"))
                        sz = float(f.get("sz"))
                        notional = px * sz
                        fee = float(f.get("fee", 0.0))
                        note_write(F_CSV, [now(), "HL", side, f"{px:.6f}", f"{sz:.6f}", f"{notional:.4f}", f"{fee:.6f}", ""])
                        # 反向對沖：若 HL 買進 => 在 Lighter 賣出（市價）
                        if lighter.enabled:
                            hedge_side_buy = (side == "sell")  # 對沖方向相反
                            status, hnote = lighter.hedge_market(CFG.lighter_symbol, hedge_side_buy, notional)
                            note_write(H_CSV, [now(), "Lighter", "HL", "buy" if hedge_side_buy else "sell", "", f"{sz:.6f}", f"{notional:.4f}", status, hnote])
                    except Exception as ie:
                        print("[WARN] 處理 fill 失敗：", ie)
            await asyncio.sleep(0.01)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("[WARN] fills_loop 中斷：", e)
    finally:
        try:
            ws.close()
        except Exception:
            pass

# ---- 收尾 ----
def shutdown():
    try:
        cancel_all_side("buy")
        cancel_all_side("sell")
        schedule_cancel_all(5)
    except Exception as e:
        print("[WARN] 收尾 cancel 失敗：", e)

def handle_sig(*_):
    shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

async def main():
    print(
        f"\n== HL Maker + Lighter Hedge ==\n"
        f"HL: {CFG.hl_network} {CFG.hl_symbol} | side={CFG.side_mode} | ${CFG.order_usd} /order | TTL={CFG.ttl_s}s | cooldown={CFG.cooldown_s}s\n"
        f"Lighter: enabled={CFG.lighter_enabled} symbol={CFG.lighter_symbol}\n"
        f"Logs -> {os.path.abspath(CFG.log_dir)}\n"
    )
    await asyncio.gather(
        maker_loop(),
        fills_loop(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        shutdown()
