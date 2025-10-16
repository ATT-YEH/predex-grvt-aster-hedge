# Adding New Exchanges

This document explains how to add support for new exchanges to the modular trading bot.

## Overview

The trading bot has been modularized to support multiple exchanges through a plugin-like architecture. Each exchange is implemented as a separate client that inherits from `BaseExchangeClient`.

## Architecture

```
exchanges/
├── __init__.py          # Module initialization
├── base.py              # Base exchange client interface
├── edgex.py             # EdgeX exchange implementation
├── backpack.py          # Backpack exchange implementation
├── paradex.py           # Paradex exchange implementation
├── aster.py             # Aster exchange implementation
├── factory.py           # Exchange factory for dynamic selection
└── your_exchange.py     # Your new exchange implementation
```

## Steps to Add a New Exchange

### 1. Create Exchange Client

Create a new file `exchanges/your_exchange.py` that implements the `BaseExchangeClient` interface:

```python
from .base import BaseExchangeClient, OrderResult, OrderInfo
from typing import Dict, Any, List, Optional

class YourExchangeClient(BaseExchangeClient):
    """Your exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Initialize your exchange-specific client here
        self.client = YourExchangeSDK(...)

    def _validate_config(self) -> None:
        """Validate exchange-specific configuration."""
        # Check for required API keys, endpoints, etc.
        pass

    async def connect(self) -> None:
        """Connect to the exchange (WebSocket, etc.)."""
        # Establish connection to your exchange
        pass

    async def disconnect(self) -> None:
        """Disconnect from the exchange."""
        # Clean up connections
        pass

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "your_exchange"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        # Set up WebSocket or polling for order updates
        pass

    async def place_open_order(self, contract_id: str, quantity: float, direction: str) -> OrderResult:
        """Place an open order."""
        # Implement order placement logic
        pass

    async def place_close_order(self, contract_id: str, quantity: float, price: float, side: str) -> OrderResult:
        """Place a close order."""
        # Implement close order placement logic
        pass

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order."""
        # Implement order cancellation logic
        pass

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information."""
        # Implement order info retrieval
        pass

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract."""
        # Implement active orders retrieval
        pass

    async def get_account_positions(self) -> Dict[str, Any]:
        """Get account positions."""
        # Implement positions retrieval
        pass
```

### 2. Register the Exchange

Add your exchange to the factory in `exchanges/factory.py`:

```python
from .your_exchange import YourExchangeClient

class ExchangeFactory:
    _registered_exchanges = {
        'edgex': EdgeXClient,
        'backpack': BackpackClient,
        'paradex': ParadexClient,
        'aster': AsterClient,
        'your_exchange': YourExchangeClient,  # Add this line
    }
```

### 3. Update Module Imports

Add your exchange to `exchanges/__init__.py`:

```python
from .your_exchange import YourExchangeClient

__all__ = ['BaseExchangeClient', 'EdgeXClient', 'BackpackClient', 'ParadexClient', 'AsterClient', 'YourExchangeClient', 'ExchangeFactory']
```

### 4. Test Your Implementation

Test your exchange client:

```python
from exchanges import ExchangeFactory

# Test exchange creation
client = ExchangeFactory.create_exchange('your_exchange', {'api_key': 'test'})
print(f"Created {client.get_exchange_name()} client")
```

## Required Methods

All exchange clients must implement these methods from `BaseExchangeClient`:

### Core Methods

- `_validate_config()` - Validate exchange-specific configuration
- `connect()` - Establish connection to exchange
- `disconnect()` - Clean up connections
- `get_exchange_name()` - Return exchange name

### Order Management

- `place_open_order(contract_id, quantity, direction)` - Place opening orders
- `place_close_order(contract_id, quantity, price, side)` - Place closing orders
- `cancel_order(order_id)` - Cancel orders
- `get_order_info(order_id)` - Get order details
- `get_active_orders(contract_id)` - Get all active orders

### Data Retrieval

- `get_account_positions()` - Get account positions
- `setup_order_update_handler(handler)` - Set up real-time order updates

## Data Structures

### OrderResult

```python
@dataclass
class OrderResult:
    success: bool
    order_id: Optional[str] = None
    side: Optional[str] = None
    size: Optional[float] = None
    price: Optional[float] = None
    status: Optional[str] = None
    error_message: Optional[str] = None
```

### OrderInfo

```python
@dataclass
class OrderInfo:
    order_id: str
    side: str
    size: float
    price: float
    status: str
    filled_size: float = 0.0
    remaining_size: float = 0.0
```

## Usage

Once implemented, users can select your exchange:

```bash
python runbot.py --exchange your_exchange --contract-id YOUR_CONTRACT_ID
```

## Best Practices

1. **Error Handling**: Always return appropriate `OrderResult` objects with error messages
2. **Async/Await**: All methods should be async for non-blocking operations
3. **Configuration**: Use environment variables for API keys and endpoints
4. **Logging**: Use the provided logger for consistent logging
5. **Testing**: Test thoroughly with paper trading before live trading
6. **Documentation**: Document any exchange-specific requirements

## Example: Binance Futures

Here's a simplified example of how you might implement Binance Futures:

```python
class BinanceFuturesClient(BaseExchangeClient):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = os.getenv('BINANCE_API_KEY')
        self.secret_key = os.getenv('BINANCE_SECRET_KEY')
        self.client = Client(self.api_key, self.secret_key)

    def _validate_config(self) -> None:
        if not self.api_key or not self.secret_key:
            raise ValueError("BINANCE_API_KEY and BINANCE_SECRET_KEY required")

    async def place_open_order(self, contract_id: str, quantity: float, direction: str) -> OrderResult:
        try:
            order = await self.client.futures_create_order(
                symbol=contract_id,
                side=direction.upper(),
                type='LIMIT',
                quantity=quantity,
                timeInForce='GTC'
            )
            return OrderResult(success=True, order_id=order['orderId'])
        except Exception as e:
            return OrderResult(success=False, error_message=str(e))
```

This modular approach makes it easy to add new exchanges while maintaining a consistent interface for the trading bot.
