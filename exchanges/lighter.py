"""
Lighter exchange client implementation.
(Updated)
- Adds place_market_order()
- Ensures market config (contract_id, tick_size, multipliers) is initialized on connect()
- get_order_info is best-effort; DO NOT use it as the source of truth for hedge completion.
  Use WebSocket order updates (account_orders) instead.
"""

import os
import asyncio
import time
import logging
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

# Official Lighter SDK
import lighter
from lighter import SignerClient, ApiClient, Configuration

# Custom WebSocket implementation
from .lighter_custom_websocket import LighterCustomWebSocketManager

# Suppress Lighter SDK debug logs
logging.getLogger("lighter").setLevel(logging.WARNING)
root_logger = logging.getLogger()
if root_logger.level == logging.DEBUG:
    root_logger.setLevel(logging.WARNING)


class LighterClient(BaseExchangeClient):
    """Lighter exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        # Credentials from environment
        self.api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY")
        self.account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self.api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

        # Base URL (mainnet)
        self.base_url = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")

        if not self.api_key_private_key:
            raise ValueError("API_KEY_PRIVATE_KEY must be set in environment variables")

        # Initialize logger
        self.logger = TradingLogger(exchange="lighter", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

        # SDK clients
        self.lighter_client: Optional[SignerClient] = None
        self.api_client: Optional[ApiClient] = None

        # Market configuration
        self.base_amount_multiplier: Optional[int] = None
        self.price_multiplier: Optional[int] = None

        # State caches (used by existing logic)
        self.orders_cache: Dict[int, Dict[str, Any]] = {}
        self.current_order_client_id: Optional[int] = None
        self.current_order: Optional[OrderInfo] = None

        # WS manager
        self.ws_manager: Optional[LighterCustomWebSocketManager] = None

    def _validate_config(self) -> None:
        required_env_vars = ["API_KEY_PRIVATE_KEY", "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def _initialize_lighter_client(self) -> SignerClient:
        """Initialize the SignerClient."""
        if self.lighter_client is None:
            try:
                self.lighter_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )

                # Best-effort check (some sdk versions expose check_client)
                if hasattr(self.lighter_client, "check_client"):
                    err = self.lighter_client.check_client()
                    if err is not None:
                        raise Exception(f"CheckClient error: {err}")

                self.logger.log("Lighter client initialized successfully", "INFO")
            except Exception as e:
                self.logger.log(f"Failed to initialize Lighter client: {e}", "ERROR")
                raise
        return self.lighter_client

    async def connect(self) -> None:
        """Connect to Lighter: init ApiClient + SignerClient + market config + WS."""
        try:
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))
            await self._initialize_lighter_client()

            # IMPORTANT: initialize market config now so multipliers/tick_size won't be None later
            await self.get_contract_attributes()

            # Add runtime fields for WS manager
            self.config.market_index = self.config.contract_id
            self.config.account_index = self.account_index
            self.config.lighter_client = self.lighter_client

            # Start custom WS manager
            self.ws_manager = LighterCustomWebSocketManager(
                config=self.config,
                order_update_callback=self._handle_websocket_order_update,
            )
            self.ws_manager.set_logger(self.logger)

            asyncio.create_task(self.ws_manager.connect())
            await asyncio.sleep(2)

        except Exception as e:
            self.logger.log(f"Error connecting to Lighter: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Lighter."""
        try:
            if self.ws_manager:
                await self.ws_manager.disconnect()
                self.ws_manager = None

            if self.api_client:
                await self.api_client.close()
                self.api_client = None

            # SignerClient may or may not have close/aclose depending on sdk version
            if self.lighter_client is not None:
                try:
                    if hasattr(self.lighter_client, "close"):
                        out = self.lighter_client.close()
                        if asyncio.iscoroutine(out):
                            await out
                except Exception:
                    pass
                self.lighter_client = None

        except Exception as e:
            self.logger.log(f"Error during Lighter disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        return "lighter"

    def setup_order_update_handler(self, handler) -> None:
        self._order_update_handler = handler

    def _handle_websocket_order_update(self, order_data_list: List[Dict[str, Any]]):
        """
        Handle order updates from WebSocket (account_orders).
        This is the correct source of truth for:
          - FILLED / PARTIALLY_FILLED / CANCELED
          - hedge completion
        """
        for order_data in order_data_list:
            if order_data.get("market_index") != self.config.contract_id:
                continue

            side = "sell" if order_data.get("is_ask") else "buy"
            order_type = "CLOSE" if side == self.config.close_order_side else "OPEN"

            order_id = order_data.get("order_index")
            status = str(order_data.get("status", "")).upper()

            # Values may be raw ints depending on WS payload; keep as Decimal for consistency
            filled_size = Decimal(str(order_data.get("filled_base_amount", "0")))
            size = Decimal(str(order_data.get("initial_base_amount", "0")))
            price = Decimal(str(order_data.get("price", "0")))
            remaining_size = Decimal(str(order_data.get("remaining_base_amount", "0")))

            # Cache compression
            if order_id in self.orders_cache:
                prev = self.orders_cache[order_id]
                if prev["status"] == "OPEN" and status == "OPEN" and filled_size == prev["filled_size"]:
                    continue
                if status in ["FILLED", "CANCELED", "CANCELLED"]:
                    del self.orders_cache[order_id]
                else:
                    prev["status"] = status
                    prev["filled_size"] = filled_size
            elif status == "OPEN":
                self.orders_cache[order_id] = {"status": status, "filled_size": filled_size}

            if status == "OPEN" and filled_size > 0:
                status = "PARTIALLY_FILLED"

            # Logging
            if status == "OPEN":
                self.logger.log(f"[{order_type}] [{order_id}] {status} {size} @ {price}", "INFO")
            else:
                self.logger.log(f"[{order_type}] [{order_id}] {status} {filled_size} @ {price}", "INFO")

            # Update current order
            try:
                if (
                    order_data.get("client_order_index") == self.current_order_client_id
                    or order_type == "OPEN"
                ):
                    self.current_order = OrderInfo(
                        order_id=str(order_id),
                        side=side,
                        size=size,
                        price=price,
                        status=status,
                        filled_size=filled_size,
                        remaining_size=remaining_size,
                        cancel_reason="",
                    )
            except Exception:
                pass

            if status in ["FILLED", "CANCELED", "CANCELLED"]:
                self.logger.log_transaction(order_id, side, filled_size, price, status)

            # Forward to external handler if set (used by your HedgeBot to detect fill)
            if self._order_update_handler:
                try:
                    self._order_update_handler(order_data)  # ✅ pass dict, not list
                except Exception as e:
                    self.logger.log(f"Order update handler error: {e}", "ERROR")

    @query_retry(default_return=(Decimal("0"), Decimal("0")))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Get best bid/ask from WS. If WS not ready -> raise."""
        if self.ws_manager and self.ws_manager.best_bid and self.ws_manager.best_ask:
            best_bid = Decimal(str(self.ws_manager.best_bid))
            best_ask = Decimal(str(self.ws_manager.best_ask))

            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                self.logger.log("Invalid bid/ask prices", "ERROR")
                raise ValueError("Invalid bid/ask prices")
            return best_bid, best_ask

        self.logger.log("Unable to get bid/ask prices from WebSocket.", "ERROR")
        raise ValueError("WebSocket not running. No bid/ask prices available")

    async def _submit_order_with_retry(self, order_params: Dict[str, Any]) -> OrderResult:
        """Submit an order using SignerClient.create_order()."""
        if self.lighter_client is None:
            raise ValueError("Lighter client not initialized. Call connect() first.")

        create_order, tx_hash, error = await self.lighter_client.create_order(**order_params)
        if error is not None:
            return OrderResult(
                success=False,
                order_id=str(order_params.get("client_order_index", "")),
                error_message=f"Order creation error: {error}",
            )

        return OrderResult(success=True, order_id=str(order_params.get("client_order_index", "")))

    async def _ensure_market_ready(self) -> None:
        """Ensure contract_id + multipliers are initialized."""
        if self.api_client is None:
            # connect() sets api_client; but allow standalone usage
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        if self.base_amount_multiplier is None or self.price_multiplier is None or not self.config.contract_id:
            await self.get_contract_attributes()

    async def place_limit_order(
        self, contract_id: str, quantity: Decimal, price: Decimal, side: str
    ) -> OrderResult:
        """Place a limit order."""
        await self._ensure_market_ready()

        if side.lower() == "buy":
            is_ask = False
        elif side.lower() == "sell":
            is_ask = True
        else:
            raise Exception(f"Invalid side: {side}")

        client_order_index = int(time.time() * 1000) % 1000000
        self.current_order_client_id = client_order_index

        order_type_limit = getattr(self.lighter_client, "ORDER_TYPE_LIMIT", "LIMIT")
        tif_gtt = getattr(self.lighter_client, "ORDER_TIME_IN_FORCE_GOOD_TILL_TIME", "GTT")

        order_params = {
            "market_index": self.config.contract_id,
            "client_order_index": client_order_index,
            "base_amount": int(quantity * self.base_amount_multiplier),
            "price": int(price * self.price_multiplier),
            "is_ask": is_ask,
            "order_type": order_type_limit,
            "time_in_force": tif_gtt,
            "reduce_only": False,
            "trigger_price": 0,
        }

        return await self._submit_order_with_retry(order_params)

    async def place_market_order(
        self, contract_id: str, quantity: Decimal, side: str
    ) -> OrderResult:
        """
        ✅ MARKET (IOC) order for hedge leg.
        NOTE: hedge completion should be determined via WebSocket order updates (FILLED/PARTIALLY_FILLED).
        """
        await self._ensure_market_ready()

        if side.lower() == "buy":
            is_ask = False
        elif side.lower() == "sell":
            is_ask = True
        else:
            raise Exception(f"Invalid side: {side}")

        client_order_index = int(time.time() * 1000) % 1000000
        self.current_order_client_id = client_order_index
        self.current_order = None

        order_type_market = getattr(self.lighter_client, "ORDER_TYPE_MARKET", "MARKET")
        tif_ioc = getattr(self.lighter_client, "ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", "IOC")

        # Many SDKs ignore price for MARKET; keep 0 to be explicit.
        order_params = {
            "market_index": self.config.contract_id,
            "client_order_index": client_order_index,
            "base_amount": int(quantity * self.base_amount_multiplier),
            "price": 0,
            "is_ask": is_ask,
            "order_type": order_type_market,
            "time_in_force": tif_ioc,
            "reduce_only": False,
            "trigger_price": 0,
        }

        return await self._submit_order_with_retry(order_params)

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Legacy helper: places a limit order near mid and waits briefly."""
        self.current_order = None
        self.current_order_client_id = None

        order_price = await self.get_order_price(direction)
        order_price = self.round_to_tick(order_price)

        order_result = await self.place_limit_order(contract_id, quantity, order_price, direction)
        if not order_result.success:
            raise Exception(f"[OPEN] Error placing order: {order_result.error_message}")

        start_time = time.time()
        order_status = "OPEN"
        while time.time() - start_time < 10 and order_status != "FILLED":
            await asyncio.sleep(0.1)
            if self.current_order is not None:
                order_status = self.current_order.status

        return OrderResult(
            success=True,
            order_id=str(self.current_order.order_id) if self.current_order else str(order_result.order_id),
            side=direction,
            size=quantity,
            price=order_price,
            status=self.current_order.status if self.current_order else "OPEN",
        )

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close (limit) order; legacy behavior kept."""
        self.current_order = None
        self.current_order_client_id = None
        order_result = await self.place_limit_order(contract_id, quantity, price, side)

        await asyncio.sleep(5)
        if order_result.success:
            return OrderResult(
                success=True,
                order_id=order_result.order_id,
                side=side,
                size=quantity,
                price=price,
                status="OPEN",
            )
        raise Exception(f"[CLOSE] Error placing order: {order_result.error_message}")

    async def get_order_price(self, side: str = "") -> Decimal:
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        order_price = (best_bid + best_ask) / 2

        active_orders = await self.get_active_orders(self.config.contract_id)
        close_orders = [o for o in active_orders if o.side == self.config.close_order_side]
        for o in close_orders:
            if side == "buy":
                order_price = min(order_price, o.price - self.config.tick_size)
            else:
                order_price = max(order_price, o.price + self.config.tick_size)

        return order_price

    async def cancel_order(self, order_id: str) -> OrderResult:
        await self._ensure_market_ready()

        cancel_order, tx_hash, error = await self.lighter_client.cancel_order(
            market_index=self.config.contract_id,
            order_index=int(order_id),
        )

        if error is not None:
            return OrderResult(success=False, error_message=f"Cancel order error: {error}")

        if tx_hash:
            return OrderResult(success=True)
        return OrderResult(success=False, error_message="Failed to send cancellation transaction")

    async def _fetch_orders_with_retry(self) -> List[Any]:
        """Fetch active orders via REST."""
        await self._ensure_market_ready()

        # auth token for REST
        if hasattr(self.lighter_client, "create_auth_token_with_expiry"):
            auth_token, error = self.lighter_client.create_auth_token_with_expiry()
        else:
            # Some SDK variants require an explicit deadline
            deadline = int(time.time() + 10 * 60)
            auth_token, error = self.lighter_client.create_auth_token_with_expiry(deadline)

        if error is not None:
            self.logger.log(f"Error creating auth token: {error}", "ERROR")
            raise ValueError(f"Error creating auth token: {error}")

        order_api = lighter.OrderApi(self.api_client)
        orders_response = await order_api.account_active_orders(
            account_index=self.account_index,
            market_id=self.config.contract_id,
            auth=auth_token,
        )

        if not orders_response:
            self.logger.log("Failed to get orders", "ERROR")
            raise ValueError("Failed to get orders")

        return orders_response.orders

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Return active orders as OrderInfo."""
        order_list = await self._fetch_orders_with_retry()
        contract_orders: List[OrderInfo] = []

        for order in order_list:
            side = "sell" if order.is_ask else "buy"
            size = Decimal(str(order.initial_base_amount))
            price = Decimal(str(order.price))
            remaining = Decimal(str(order.remaining_base_amount))
            filled = Decimal(str(order.filled_base_amount))

            if remaining > 0:
                contract_orders.append(
                    OrderInfo(
                        order_id=str(order.order_index),
                        side=side,
                        size=size,  # FIX: size should be initial size
                        price=price,
                        status=str(order.status).upper(),
                        filled_size=filled,
                        remaining_size=remaining,
                    )
                )

        return contract_orders

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """
        Best-effort order lookup.
        ⚠️ IMPORTANT: Do NOT use this to decide hedge completion.
        Use WebSocket (account_orders) updates instead.
        """
        try:
            # Try active orders first
            active = await self.get_active_orders(self.config.contract_id)
            for o in active:
                if str(o.order_id) == str(order_id):
                    return o

            # If it's not active, it might already be filled/canceled.
            # Many SDKs don't provide a direct "order by id" endpoint.
            # We return None here and rely on WS fill events.
            return None
        except Exception as e:
            self.logger.log(f"Error getting order info (best-effort): {e}", "ERROR")
            return None

    async def _fetch_positions_with_retry(self) -> List[Any]:
        """Fetch account positions."""
        if self.api_client is None:
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

        account_api = lighter.AccountApi(self.api_client)
        account_data = await account_api.account(by="index", value=str(self.account_index))
        if not account_data or not getattr(account_data, "accounts", None):
            self.logger.log("Failed to get positions", "ERROR")
            raise ValueError("Failed to get positions")
        return account_data.accounts[0].positions

    async def get_account_positions(self) -> Decimal:
        positions = await self._fetch_positions_with_retry()
        for position in positions:
            if position.market_id == self.config.contract_id:
                return Decimal(str(position.position))
        return Decimal("0")

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Initialize contract_id, tick_size, multipliers for the configured ticker."""
        if not self.config.ticker:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        if self.api_client is None:
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

        order_api = lighter.OrderApi(self.api_client)
        order_books = await order_api.order_books()

        market_info = None
        for market in order_books.order_books:
            if market.symbol == self.config.ticker:
                market_info = market
                break

        if market_info is None:
            self.logger.log(f"Ticker not found in markets: {self.config.ticker}", "ERROR")
            raise ValueError("Failed to get markets")

        market_summary = await order_api.order_book_details(market_id=market_info.market_id)
        order_book_details = market_summary.order_book_details[0]

        self.config.contract_id = market_info.market_id
        self.base_amount_multiplier = int(pow(10, market_info.supported_size_decimals))
        self.price_multiplier = int(pow(10, market_info.supported_price_decimals))

        try:
            self.config.tick_size = Decimal("1") / (Decimal("10") ** Decimal(str(order_book_details.price_decimals)))
        except Exception:
            self.logger.log("Failed to get tick size", "ERROR")
            raise ValueError("Failed to get tick size")

        self.logger.log(
            f"Market ready: ticker={self.config.ticker} contract_id={self.config.contract_id} "
            f"tick={self.config.tick_size} base_mul={self.base_amount_multiplier} price_mul={self.price_multiplier}",
            "INFO",
        )

        return self.config.contract_id, self.config.tick_size
