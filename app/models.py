from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Candle:
    symbol: str
    security_id: str
    timeframe: int
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0

    def update(self, price: float, volume_delta: int = 0) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += max(0, int(volume_delta or 0))

    def is_green(self) -> bool:
        return self.close > self.open

    def is_red(self) -> bool:
        return self.close < self.open

    def size_percent(self) -> float:
        if self.open <= 0:
            return 0.0
        return abs(self.high - self.low) / self.open * 100

    def close_location_percent(self) -> float:
        spread = self.high - self.low
        if spread <= 0:
            return 100.0
        return (self.close - self.low) / spread * 100

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "security_id": self.security_id,
            "timeframe": self.timeframe,
            "start": self.start.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


@dataclass
class Instrument:
    symbol: str
    security_id: str
    exchange_segment: str = "NSE_EQ"
    instrument: str = "EQUITY"
    sector: str = ""


@dataclass
class Position:
    symbol: str
    security_id: str
    side: str
    strategy: str
    entry_price: float
    quantity: int
    stop_loss: float
    target: float
    opened_at: str
    status: str = "OPEN"
    order_id: str = ""
    exit_order_id: str = ""
    exit_reason: str = ""
    last_price: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()
