from .models import Candle


def ema(values: list[float], period: int) -> float | None:
    if not values:
        return None
    period = max(1, int(period))
    multiplier = 2 / (period + 1)
    value = values[0]
    for price in values[1:]:
        value = price * multiplier + value * (1 - multiplier)
    return value


def candle_ema(candles: list[Candle], period: int = 10) -> float | None:
    return ema([c.close for c in candles if c.close > 0], period)


def vwap(candles: list[Candle]) -> float | None:
    amount = 0.0
    volume = 0
    for candle in candles:
        typical = (candle.high + candle.low + candle.close) / 3
        amount += typical * candle.volume
        volume += candle.volume
    if volume <= 0:
        return None
    return amount / volume
