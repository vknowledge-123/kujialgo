from collections import defaultdict, deque
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

from .config import IST_TIMEZONE
from .models import Candle, Instrument

IST = ZoneInfo(IST_TIMEZONE)
SESSION_OPEN = dtime(9, 15)
SESSION_CLOSE = dtime(15, 30)


def to_ist(value: datetime | int | float | str | None = None) -> datetime:
    if value is None:
        return datetime.now(IST)
    if isinstance(value, datetime):
        return value.astimezone(IST) if value.tzinfo else value.replace(tzinfo=IST)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, IST)
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(IST) if parsed.tzinfo else parsed.replace(tzinfo=IST)
    except ValueError:
        return datetime.now(IST)


def floor_timeframe(dt: datetime, timeframe: int) -> datetime:
    dt = to_ist(dt).replace(second=0, microsecond=0)
    session_start = dt.replace(hour=9, minute=15)
    minutes = max(0, int((dt - session_start).total_seconds() // 60))
    bucket = (minutes // timeframe) * timeframe
    return session_start + timedelta(minutes=bucket)


def expected_starts_for_day(day: datetime, timeframe: int) -> list[datetime]:
    start = to_ist(day).replace(hour=9, minute=15, second=0, microsecond=0)
    end = to_ist(day).replace(hour=15, minute=30, second=0, microsecond=0)
    values = []
    current = start
    while current < end:
        values.append(current)
        current += timedelta(minutes=timeframe)
    return values


class CandleStore:
    def __init__(self, max_closed: int = 600):
        self.current: dict[tuple[str, int], Candle] = {}
        self.closed: dict[tuple[str, int], deque[Candle]] = defaultdict(lambda: deque(maxlen=max_closed))
        self.last_cumulative_volume: dict[str, int] = {}

    def seed_history(self, instrument: Instrument, timeframe: int, rows: list[dict]) -> None:
        key = (instrument.symbol, timeframe)
        for row in rows:
            start = floor_timeframe(to_ist(row.get("timestamp")), timeframe)
            candle = Candle(
                symbol=instrument.symbol,
                security_id=instrument.security_id,
                timeframe=timeframe,
                start=start,
                open=float(row.get("open") or 0),
                high=float(row.get("high") or 0),
                low=float(row.get("low") or 0),
                close=float(row.get("close") or 0),
                volume=int(float(row.get("volume") or 0)),
            )
            if candle.open > 0:
                self._replace_closed(key, candle)

    def on_tick(self, instrument: Instrument, price: float, cumulative_volume: int | None, timestamp: datetime | None = None) -> list[Candle]:
        dt = to_ist(timestamp)
        if not (SESSION_OPEN <= dt.time() <= SESSION_CLOSE):
            return []
        closed_now: list[Candle] = []
        volume_delta = 0
        if cumulative_volume is not None:
            last = self.last_cumulative_volume.get(instrument.security_id)
            cumulative = int(cumulative_volume)
            volume_delta = max(0, cumulative - last) if last is not None else 0
            self.last_cumulative_volume[instrument.security_id] = cumulative
        for timeframe in (1, 5):
            start = floor_timeframe(dt, timeframe)
            key = (instrument.symbol, timeframe)
            candle = self.current.get(key)
            if not candle or candle.start != start:
                if candle:
                    self.closed[key].append(candle)
                    closed_now.append(candle)
                candle = Candle(
                    symbol=instrument.symbol,
                    security_id=instrument.security_id,
                    timeframe=timeframe,
                    start=start,
                    open=float(price),
                    high=float(price),
                    low=float(price),
                    close=float(price),
                    volume=0,
                )
                self.current[key] = candle
            candle.update(float(price), volume_delta)
        return closed_now

    def all_candles(self, symbol: str, timeframe: int, include_current: bool = True) -> list[Candle]:
        key = (symbol, timeframe)
        rows = list(self.closed.get(key, []))
        current = self.current.get(key)
        if include_current and current:
            rows.append(current)
        return rows

    def closed_candles(self, symbol: str, timeframe: int) -> list[Candle]:
        return list(self.closed.get((symbol, timeframe), []))

    def latest_price(self, symbol: str) -> float:
        current = self.current.get((symbol, 1)) or self.current.get((symbol, 5))
        return float(current.close) if current else 0.0

    def _replace_closed(self, key: tuple[str, int], candle: Candle) -> None:
        existing = [item for item in self.closed[key] if item.start != candle.start]
        existing.append(candle)
        existing.sort(key=lambda item: item.start)
        self.closed[key].clear()
        self.closed[key].extend(existing[-self.closed[key].maxlen :])
