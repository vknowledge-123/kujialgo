from dataclasses import dataclass, field
from datetime import datetime
import math
from typing import Any

from .indicators import candle_ema, vwap
from .models import Candle, Position


@dataclass
class StrategySettings:
    timeframe: int = 5
    quantity: int = 1
    sizing_mode: str = "capital"
    per_trade_capital: float = 10000.0
    per_trade_risk: float = 20.0
    dry_run: bool = True
    sma_period: int = 20
    near_high_percent: float = 70.0
    volume_multiplier: float = 8.0
    fixed_sl_percent: float = 0.7
    scalper_sl_percent: float = 0.8
    scalper_pyramiding: bool = False
    scalper_max_adds: int = 2
    auto_square_enabled: bool = True
    auto_square_time: str = "15:09"
    candle_sl_max_percent: float = 1.0
    risk_reward: float = 3.0
    ema_period: int = 10
    use_sector_filter: bool = False
    top_sector_count: int = 2
    enabled: dict[str, bool] = field(default_factory=lambda: {
        "long_s1": True,
        "long_s2": True,
        "long_s3": True,
        "long_s4": True,
        "scalper_long": False,
        "scalper_short": False,
        "short_s1": False,
        "short_s2": False,
        "short_s3": False,
        "short_s4": False,
    })

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "StrategySettings":
        current = cls()
        enabled = dict(current.enabled)
        enabled.update(payload.get("enabled") or {})
        sizing_mode = str(payload.get("sizing_mode") or current.sizing_mode).lower()
        if sizing_mode not in {"capital", "risk"}:
            sizing_mode = current.sizing_mode
        return cls(
            timeframe=5 if int(payload.get("timeframe") or current.timeframe) == 5 else 1,
            quantity=max(1, int(payload.get("quantity") or current.quantity)),
            sizing_mode=sizing_mode,
            per_trade_capital=max(0.0, float(payload.get("per_trade_capital") or current.per_trade_capital)),
            per_trade_risk=max(0.0, float(payload.get("per_trade_risk") or current.per_trade_risk)),
            dry_run=bool(payload.get("dry_run", current.dry_run)),
            sma_period=max(1, int(payload.get("sma_period") or current.sma_period)),
            near_high_percent=float(payload.get("near_high_percent") or current.near_high_percent),
            volume_multiplier=float(payload.get("volume_multiplier") or current.volume_multiplier),
            fixed_sl_percent=float(payload.get("fixed_sl_percent") or current.fixed_sl_percent),
            scalper_sl_percent=max(0.1, float(payload.get("scalper_sl_percent") or current.scalper_sl_percent)),
            scalper_pyramiding=bool(payload.get("scalper_pyramiding", current.scalper_pyramiding)),
            scalper_max_adds=max(0, int(payload.get("scalper_max_adds") if payload.get("scalper_max_adds") is not None else current.scalper_max_adds)),
            auto_square_enabled=bool(payload.get("auto_square_enabled", current.auto_square_enabled)),
            auto_square_time=normalize_time_setting(payload.get("auto_square_time") or current.auto_square_time, current.auto_square_time),
            candle_sl_max_percent=float(payload.get("candle_sl_max_percent") or current.candle_sl_max_percent),
            risk_reward=max(0.1, float(payload.get("risk_reward") or current.risk_reward)),
            ema_period=max(1, int(payload.get("ema_period") or current.ema_period)),
            use_sector_filter=bool(payload.get("use_sector_filter", current.use_sector_filter)),
            top_sector_count=max(1, int(payload.get("top_sector_count") or current.top_sector_count)),
            enabled=enabled,
        )


def normalize_time_setting(value: Any, default: str) -> str:
    text = str(value or "").strip()
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text[:2])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return f"{hour:02d}:{minute:02d}"
    except (TypeError, ValueError):
        pass
    return default


def volume_sma(baseline: list[int], period: int) -> float:
    values = [int(v or 0) for v in baseline if int(v or 0) > 0]
    if not values:
        return 0.0
    subset = values[-period:]
    return sum(subset) / len(subset)


def setup_stop(entry: float, candle: Candle, settings: StrategySettings) -> tuple[float, float]:
    fixed_stop = entry * (1 - settings.fixed_sl_percent / 100)
    candle_size = candle.size_percent()
    stop = candle.low if candle_size <= settings.candle_sl_max_percent else fixed_stop
    risk = max(0.01, entry - stop)
    target = entry + risk * settings.risk_reward
    return round(stop, 2), round(target, 2)


def calculate_quantity(entry: float, stop: float, settings: StrategySettings, side: str = "LONG") -> tuple[int, str]:
    entry = float(entry or 0)
    stop = float(stop or 0)
    if entry <= 0:
        return 0, "invalid entry price"
    if settings.sizing_mode == "risk":
        per_share_risk = abs(entry - stop)
        if per_share_risk <= 0:
            return 0, "invalid risk because entry and stop loss are equal"
        quantity = math.floor(settings.per_trade_risk / per_share_risk)
        return quantity, f"{side.lower()} risk sizing: {settings.per_trade_risk:.2f} / {per_share_risk:.2f}"
    quantity = math.floor(settings.per_trade_capital / entry)
    if quantity < 1:
        quantity = 1
    return quantity, f"capital sizing: {settings.per_trade_capital:.2f} / {entry:.2f}"


class LongStrategyEvaluator:
    def __init__(self):
        self.armed: dict[str, dict[str, Any]] = {}
        self.triggered: set[tuple[str, str]] = set()

    def on_closed_candle(
        self,
        symbol: str,
        candles: list[Candle],
        previous_day: dict[str, float],
        baseline_volumes: list[int],
        settings: StrategySettings,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        if not candles:
            return events
        first = candles[0]
        baseline = volume_sma(baseline_volumes, settings.sma_period)
        first_common = self._first_candle_ok(first, previous_day, baseline, settings)
        first_green_common = first_common and first.is_green()
        first_red_common = first_common and first.is_red()
        if first_green_common and len(candles) >= 2:
            second = candles[1]
            if settings.enabled.get("long_s1") and second.is_green():
                events.append(self._arm(symbol, "long_s1", second, "Second candle green breakout"))
            if settings.enabled.get("long_s2") and second.is_red():
                events.append(self._arm(symbol, "long_s2", second, "Second candle red high breakout"))
        if first_red_common and len(candles) >= 3:
            second, third = candles[1], candles[2]
            if settings.enabled.get("long_s3") and second.is_green() and third.is_green():
                events.append(self._arm(symbol, "long_s3", third, "Third candle high breakout"))
        if first_green_common and len(candles) >= 2 and settings.enabled.get("long_s4"):
            event = self._strategy4(symbol, candles, settings)
            if event:
                events.append(event)
        return events

    def evaluate_tick_entry(
        self,
        symbol: str,
        price: float,
        candles: list[Candle],
        settings: StrategySettings,
    ) -> dict[str, Any] | None:
        if price <= 0:
            return None
        ema10 = candle_ema(candles, settings.ema_period)
        day_vwap = vwap(candles)
        if ema10 is not None and price <= ema10:
            return None
        if day_vwap is not None and price <= day_vwap:
            return None
        for key, state in list(self.armed.items()):
            state_symbol, strategy = key.split(":", 1)
            if state_symbol != symbol:
                continue
            if (symbol, strategy) in self.triggered:
                continue
            if price > state["trigger_high"]:
                self.triggered.add((symbol, strategy))
                return {
                    "symbol": symbol,
                    "strategy": strategy,
                    "side": "LONG",
                    "entry_price": price,
                    "stop_candle": state["stop_candle"],
                    "reason": state["reason"],
                }
        return None

    def evaluate_exit(self, position: Position, candles: list[Candle], settings: StrategySettings) -> str:
        price = candles[-1].close if candles else position.last_price
        if price and price <= position.stop_loss:
            return "STOP_LOSS"
        if price and price >= position.target:
            return "TARGET"
        last = candles[-1] if candles else None
        ema10 = candle_ema(candles, settings.ema_period)
        if last and ema10 is not None and last.is_red() and last.close < ema10:
            return "RED_CLOSE_BELOW_EMA"
        return ""

    def _first_candle_ok(self, first: Candle, previous_day: dict[str, float], baseline: float, settings: StrategySettings) -> bool:
        previous_high = float(previous_day.get("high") or 0)
        volume_ok = baseline <= 0 or first.volume >= baseline * settings.volume_multiplier
        return (
            first.close > previous_high > 0
            and first.close_location_percent() >= settings.near_high_percent
            and volume_ok
        )

    def _arm(self, symbol: str, strategy: str, trigger_candle: Candle, reason: str) -> dict[str, Any]:
        key = f"{symbol}:{strategy}"
        self.armed[key] = {
            "trigger_high": trigger_candle.high,
            "stop_candle": trigger_candle,
            "reason": reason,
            "armed_at": datetime.now().isoformat(),
        }
        return {"type": "ARMED", "symbol": symbol, "strategy": strategy, "trigger": trigger_candle.high, "reason": reason}

    def _strategy4(self, symbol: str, candles: list[Candle], settings: StrategySettings) -> dict[str, Any] | None:
        breakout = candles[-1]
        if not breakout.is_green():
            return None
        red_candles = [c for c in candles[1:-1] if c.is_red()]
        if not red_candles:
            return None
        last_red = red_candles[-1]
        min_volume = min(c.volume for c in candles[:-1]) if candles[:-1] else last_red.volume
        ema10 = candle_ema(candles, settings.ema_period)
        day_vwap = vwap(candles)
        trend_ok = (ema10 is None or breakout.close > ema10) and (day_vwap is None or breakout.close > day_vwap)
        volume_condition = last_red.volume <= min_volume
        trend_condition = breakout.close > last_red.high and trend_ok
        if breakout.close > last_red.high and (volume_condition or trend_condition):
            self.triggered.add((symbol, "long_s4"))
            return {
                "type": "ENTRY",
                "symbol": symbol,
                "strategy": "long_s4",
                "side": "LONG",
                "entry_price": breakout.close,
                "stop_candle": breakout,
                "reason": "Green candle closed above last red candle high",
            }
        return None
