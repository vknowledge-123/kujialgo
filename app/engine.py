import asyncio
import inspect
import json
import struct
import threading
import time
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import websockets

from .candles import CandleStore, IST, SESSION_OPEN, expected_starts_for_day, floor_timeframe, to_ist
from .config import (
    ALLOW_POSITIONS_ONLY_RECONCILE,
    BROKER_ORDER_BOOK_TIMEOUT_SECONDS,
    BROKER_PENDING_RECOVERY_TIMEOUT_SECONDS,
    BROKER_POSITIONS_TIMEOUT_SECONDS,
    BROKER_PENDING_LOOKUP_LIMIT,
    BROKER_RECONCILE_TIMEOUT_SECONDS,
    BROKER_TRADE_BOOK_TIMEOUT_SECONDS,
    DHAN_FEED_URL,
    DHAN_ORDER_UPDATE_URL,
    EXECUTION_WORKER_COUNT,
    MAX_INSTRUMENTS_PER_CONNECTION,
    MAX_MARKET_FEED_CONNECTIONS,
    MAX_SUBSCRIBE_BATCH,
    ORDER_SOCKET_OPEN_TIMEOUT_SECONDS,
    ORDER_SOCKET_PING_INTERVAL_SECONDS,
    ORDER_SOCKET_PING_TIMEOUT_SECONDS,
    ORDER_SOCKET_RECONNECT_BASE_SECONDS,
    ORDER_SOCKET_RECONNECT_MAX_SECONDS,
    PREMARKET_FILE,
    PREMARKET_REPORT_FILE,
    RECONCILED_CANDLES_FILE,
    RECONCILE_SNAPSHOT_FILE,
    STATE_FILE,
    TICK_QUEUE_MAXSIZE,
    TICK_QUEUE_WARN_INTERVAL_SECONDS,
    TRADE_LEDGER_FILE,
)
from .dhan_api import DhanAuthenticationError, DhanClient
from .models import Candle, Instrument, Position
from .storage import read_json, write_json
from .strategy import LongStrategyEvaluator, StrategySettings, calculate_quantity, setup_stop
from .symbols import (
    InstrumentResolver,
    extract_symbols,
    normalize_symbol,
    sector_index_instruments,
    sectors_for_symbol,
)

EXCHANGE_SEGMENT_CODES = {0: "IDX_I", 1: "NSE_EQ", 4: "BSE_EQ"}
TRADED_STATUSES = {"TRADED", "COMPLETED"}
PARTIAL_FILL_STATUSES = {"PARTIAL", "PART_TRADED", "PARTIALLY_FILLED", "PARTIALLY_TRADED"}
FILL_ACCEPTED_STATUSES = TRADED_STATUSES | PARTIAL_FILL_STATUSES
FAILED_STATUSES = {"REJECTED", "CANCELLED", "EXPIRED", "FAILED", "PLACE_FAILED", "STALE_UNRESOLVED"}
PENDING_STATUSES = {"TRANSIT", "PENDING"}
FINAL_ORDER_STATUSES = TRADED_STATUSES | PARTIAL_FILL_STATUSES | FAILED_STATUSES
PENDING_LEDGER_STATUSES = {"PENDING_ENTRY", "PENDING_EXIT", "PENDING_PYRAMID", "PENDING_ORDER", "TRANSIT", "PENDING"}


def _today_key() -> str:
    return datetime.now(IST).date().isoformat()


def _empty_ledger() -> dict[str, Any]:
    return {
        "session_date": _today_key(),
        "orders": {},
        "positions": {},
        "updated_at": datetime.now(IST).isoformat(),
    }


def _as_list_payload(data: Any) -> list[dict[str, Any]]:
    payload = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        payload = payload["data"]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _broker_order_id(row: dict[str, Any]) -> str:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    return str(_broker_value(row, "orderId", "OrderNo", "OrderId", "orderNo", "order_id") or "")


def _broker_correlation_id(row: dict[str, Any]) -> str:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    return str(_broker_value(row, "correlationId", "CorrelationId", "CorrelationID", "correlation_id") or "")


def _broker_order_status(row: dict[str, Any], fallback: str = "") -> str:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    return str(_broker_value(row, "orderStatus", "Status", "OrderStatus", "status") or fallback or "").upper()


def _broker_value(row: dict[str, Any], *keys: str) -> Any:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(str(key).lower())
        if value not in (None, ""):
            return value
    return None


def _broker_quantity(row: dict[str, Any], *keys: str) -> int:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    for key in keys:
        value = _broker_value(row, key)
        if value not in (None, ""):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                continue
    return 0


def _broker_price(row: dict[str, Any], *keys: str) -> float:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    for key in keys:
        value = _broker_value(row, key)
        if value not in (None, ""):
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return 0.0


def _broker_filled_quantity(row: dict[str, Any]) -> int:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    filled = _broker_quantity(row, "filledQty", "filledQuantity", "tradedQuantity", "tradedQty", "TradedQty")
    if filled > 0:
        return filled
    requested = _broker_quantity(row, "quantity", "Quantity", "orderQuantity")
    remaining_value = _broker_value(row, "remainingQuantity", "RemainingQuantity", "remainingQty", "pendingQuantity")
    if remaining_value not in (None, ""):
        try:
            remaining = int(float(remaining_value))
        except (TypeError, ValueError):
            remaining = -1
        if requested > 0 and remaining >= 0 and requested >= remaining:
            return requested - remaining
    status = _broker_order_status(row)
    average = _broker_price(row, "averageTradedPrice", "AvgTradedPrice", "averagePrice", "tradedPrice", "TradedPrice")
    if status in TRADED_STATUSES and requested > 0 and average > 0:
        return requested
    return 0


def _broker_security_id(row: dict[str, Any]) -> str:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    return str(_broker_value(row, "securityId", "SecurityId", "security_id") or "")


def _broker_product_type(row: dict[str, Any]) -> str:
    row = row.get("data", row) if isinstance(row.get("data"), dict) else row
    value = str(_broker_value(row, "productType", "ProductName", "Product", "product_type") or "").upper()
    if value == "I":
        return "INTRADAY"
    if value == "C":
        return "CNC"
    if value == "M":
        return "MARGIN"
    if value == "F":
        return "MTF"
    if value == "V":
        return "CO"
    if value == "B":
        return "BO"
    return value


def _broker_transaction_type(row: dict[str, Any]) -> str:
    value = str(_broker_value(row, "transactionType", "TxnType", "transaction_type") or "").upper()
    if value == "B":
        return "BUY"
    if value == "S":
        return "SELL"
    return value


def _json_objects_from_message(message: str | bytes) -> list[Any]:
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError:
            return []
    text = str(message or "").strip()
    if not text:
        return []
    try:
        return [json.loads(text)]
    except json.JSONDecodeError:
        pass
    rows = []
    decoder = json.JSONDecoder()
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        try:
            item, end = decoder.raw_decode(text, index)
            rows.append(item)
            index = end
        except json.JSONDecodeError:
            next_line = text.find("\n", index)
            if next_line < 0:
                break
            index = next_line + 1
    return rows


def _is_auth_error(exc: Exception) -> bool:
    text = str(exc)
    return "DH-901" in text or "Invalid_Authentication" in text or "invalid or expired" in text


def _row_date(row: dict[str, Any]) -> str:
    timestamp = row.get("timestamp")
    return to_ist(timestamp).date().isoformat() if timestamp else ""


def _latest_completed_daily(rows: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    today = now.date()
    completed = [row for row in rows if row.get("timestamp") and to_ist(row.get("timestamp")).date() < today]
    if completed:
        return completed[-1]
    return rows[-2] if len(rows) > 1 else (rows[-1] if rows else {})


def _aggregate_intraday_rows(rows: list[dict[str, Any]], timeframe: int) -> list[dict[str, Any]]:
    buckets: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        timestamp = row.get("timestamp")
        if not timestamp:
            continue
        start = to_ist(timestamp).replace(second=0, microsecond=0)
        session_start = start.replace(hour=9, minute=15)
        minutes = int((start - session_start).total_seconds() // 60)
        if minutes < 0:
            continue
        bucket_start = session_start + timedelta(minutes=(minutes // timeframe) * timeframe)
        bucket = buckets.get(bucket_start)
        if not bucket:
            buckets[bucket_start] = {
                "timestamp": bucket_start.isoformat(),
                "open": float(row.get("open") or 0),
                "high": float(row.get("high") or 0),
                "low": float(row.get("low") or 0),
                "close": float(row.get("close") or 0),
                "volume": int(row.get("volume") or 0),
            }
        else:
            bucket["high"] = max(bucket["high"], float(row.get("high") or 0))
            bucket["low"] = min(bucket["low"], float(row.get("low") or 0))
            bucket["close"] = float(row.get("close") or 0)
            bucket["volume"] += int(row.get("volume") or 0)
    return [buckets[key] for key in sorted(buckets)]


def _cache_updated_today(entry: dict[str, Any], today_key: str) -> bool:
    return str(entry.get("updated_at") or "").startswith(today_key)


def _symbol_cache_ready(entry: dict[str, Any], today_key: str, required_bars: int) -> bool:
    baseline = entry.get("baseline") or {}
    previous = entry.get("previous_day") or {}
    return (
        _cache_updated_today(entry, today_key)
        and float(previous.get("close") or 0) > 0
        and len(baseline.get("1") or []) >= required_bars
        and len(baseline.get("5") or []) >= required_bars
    )


def _sector_cache_ready(entry: dict[str, Any], today_key: str) -> bool:
    previous = entry.get("previous_day") or {}
    return _cache_updated_today(entry, today_key) and float(previous.get("close") or 0) > 0


def _first_items(items: list[str], limit: int = 80) -> list[str]:
    return sorted(items)[:limit]


def _premarket_cache_summary(
    cache: dict[str, Any],
    stock_instruments: list[Instrument],
    sector_items: list[tuple[str, Instrument]],
    today_key: str,
    required_bars: int,
) -> dict[str, Any]:
    symbols_cache = cache.get("symbols") or {}
    sectors_cache = cache.get("sectors") or {}
    errors = cache.get("errors") or {}
    stock_symbols = sorted({instrument.symbol for instrument in stock_instruments})
    sector_names = sorted({name for name, _instrument in sector_items})
    missing_stocks = [symbol for symbol in stock_symbols if not _symbol_cache_ready(symbols_cache.get(symbol) or {}, today_key, required_bars)]
    missing_sectors = [name for name in sector_names if not _sector_cache_ready(sectors_cache.get(name) or {}, today_key)]
    failed_symbols = sorted([key for key in errors if key in stock_symbols])
    failed_sectors = sorted([key for key in errors if key in sector_names])
    return {
        "stock_total": len(stock_symbols),
        "stock_cached": len(stock_symbols) - len(missing_stocks),
        "stock_missing": len(missing_stocks),
        "stock_missing_symbols": _first_items(missing_stocks),
        "stock_missing_all": sorted(missing_stocks),
        "sector_total": len(sector_names),
        "sector_cached": len(sector_names) - len(missing_sectors),
        "sector_missing": len(missing_sectors),
        "sector_missing_symbols": _first_items(missing_sectors),
        "sector_missing_all": sorted(missing_sectors),
        "failed_symbols": _first_items(failed_symbols),
        "failed_symbols_all": failed_symbols,
        "failed_sectors": _first_items(failed_sectors),
        "failed_sectors_all": failed_sectors,
        "error_count": len(errors),
    }


class DhanAlgoEngine:
    def __init__(self, enable_reconcile_workers: bool = True, enable_snapshot_worker: bool = True):
        self.enable_reconcile_workers = enable_reconcile_workers
        self.enable_snapshot_worker = enable_snapshot_worker
        self.resolver = InstrumentResolver()
        self.client: DhanClient | None = None
        self.settings = StrategySettings()
        self.candles = CandleStore()
        self.long_evaluator = LongStrategyEvaluator()
        self.running = False
        self.market_connected = False
        self.market_connecting = False
        self.order_connected = False
        self.order_connecting = False
        self.order_last_error = ""
        self.order_reconnects = 0
        self.last_error = ""
        self.last_tick_ts = 0.0
        self.feed_generation = 0
        self.loop: asyncio.AbstractEventLoop | None = None
        self.tick_queue: asyncio.Queue[tuple[str, str, float, int | None, datetime]] | None = None
        self.dropped_ticks = 0
        self.last_tick_queue_warn_ts = 0.0
        self.feed_threads: list[threading.Thread] = []
        self.feed_sockets: list[Any] = []
        self.order_task: asyncio.Task | None = None
        self.tick_task: asyncio.Task | None = None
        self.execution_task: asyncio.Task | None = None
        self.execution_tasks: set[asyncio.Task] = set()
        self.snapshot_task: asyncio.Task | None = None
        self.cache_task: asyncio.Task | None = None
        self.reconcile_task: asyncio.Task | None = None
        self.broker_reconcile_task: asyncio.Task | None = None
        self.action_tasks: set[asyncio.Task] = set()
        self.execution_queue: asyncio.PriorityQueue[tuple[int, int, str, Any]] | None = None
        self.action_sequence = 0
        self.symbol_action_locks: dict[str, asyncio.Lock] = {}
        self.snapshot_cache: dict[str, Any] = {}
        self.snapshot_cache_ts = 0.0
        self.pending_actions: set[str] = set()
        self.credentials: dict[str, str] = {}
        self.long_symbols: list[str] = []
        self.short_symbols: list[str] = []
        self.universe_symbols: set[str] = set()
        self.instruments_by_symbol: dict[str, Instrument] = {}
        self.instruments_by_security: dict[str, Instrument] = {}
        self.sector_instruments = sector_index_instruments()
        self.sector_security_to_name = {item.security_id: name for name, item in self.sector_instruments.items()}
        self.sector_live: dict[str, dict[str, Any]] = {}
        self.order_updates: dict[str, dict[str, Any]] = {}
        self.positions: dict[str, Position] = {}
        self.scalper_state: dict[str, dict[str, Any]] = {}
        self.reconciled_day_history: set[str] = set()
        self.ledger: dict[str, Any] = _empty_ledger()
        self.locked_symbols: set[str] = set()
        self.external_locked_symbols: set[str] = set()
        self.last_external_state_load = 0.0
        self.last_reconciled_candle_load = ""
        self.premarket_cache_data: dict[str, Any] = {"symbols": {}, "sectors": {}, "errors": {}}
        self.premarket_cache_mtime = 0.0
        self.premarket_cache_checked_at = 0.0
        self.entries_blocked_until_reconcile = False
        self.broker_positions: dict[str, dict[str, Any]] = {}
        self.broker_reconcile_status: dict[str, Any] = {
            "running": False,
            "message": "Not checked",
            "last_run": "",
            "mismatches": [],
            "pending_orders": [],
            "failed_orders": [],
            "locked_symbols": [],
            "entries_blocked_until_reconcile": False,
        }
        self.events: list[dict[str, Any]] = []
        self.premarket_status: dict[str, Any] = {"running": False, "message": "Not started", "progress": 0}
        self.reconcile_status: dict[str, Any] = {"running": False, "message": "Not started", "last_run": "", "last_missing": {}}
        self.lock = threading.RLock()
        self._load_state()
        self._load_ledger()
        self._load_positions_from_ledger()

    def _load_state(self) -> None:
        state = read_json(STATE_FILE, {})
        self.credentials = state.get("credentials") or {}
        self.long_symbols = state.get("long_symbols") or []
        self.short_symbols = state.get("short_symbols") or []
        self.universe_symbols = set(state.get("universe_symbols") or [])
        self.settings = StrategySettings.from_payload(state.get("settings") or {})

    def save_state(self) -> None:
        write_json(
            STATE_FILE,
            {
                "credentials": self.credentials,
                "long_symbols": self.long_symbols,
                "short_symbols": self.short_symbols,
                "universe_symbols": sorted(self.universe_symbols),
                "settings": self.settings.__dict__,
            },
        )

    def _load_ledger(self) -> None:
        ledger = read_json(TRADE_LEDGER_FILE, _empty_ledger())
        if not isinstance(ledger, dict) or ledger.get("session_date") != _today_key():
            ledger = _empty_ledger()
        ledger.setdefault("orders", {})
        ledger.setdefault("positions", {})
        self.ledger = ledger

    def _save_ledger(self) -> None:
        self.ledger["updated_at"] = datetime.now(IST).isoformat()
        write_json(TRADE_LEDGER_FILE, self.ledger)

    def _load_positions_from_ledger(self) -> None:
        loaded: dict[str, Position] = {}
        for key, row in (self.ledger.get("positions") or {}).items():
            if not isinstance(row, dict) or row.get("status") != "OPEN":
                continue
            try:
                loaded[key] = Position(
                    symbol=str(row.get("symbol") or ""),
                    security_id=str(row.get("security_id") or ""),
                    side=str(row.get("side") or "LONG").upper(),
                    strategy=str(row.get("strategy") or "recovered"),
                    entry_price=float(row.get("entry_price") or 0),
                    quantity=int(row.get("quantity") or 0),
                    stop_loss=float(row.get("stop_loss") or 0),
                    target=float(row.get("target") or 0),
                    opened_at=str(row.get("opened_at") or datetime.now(IST).isoformat()),
                    status=str(row.get("status") or "OPEN"),
                    order_id=str(row.get("order_id") or ""),
                    exit_order_id=str(row.get("exit_order_id") or ""),
                    exit_reason=str(row.get("exit_reason") or ""),
                    last_price=float(row.get("last_price") or 0),
                    meta=dict(row.get("meta") or {}),
                )
            except (TypeError, ValueError):
                continue
        self.positions = loaded

    def _position_key(self, symbol: str, side: str) -> str:
        return f"{symbol}:{side.upper()}"

    def _persist_position(self, position: Position) -> None:
        key = self._position_key(position.symbol, position.side)
        self.ledger.setdefault("positions", {})[key] = position.as_dict()
        self._save_ledger()

    def _ledger_order_status_for_role(self, order_role: str) -> str:
        role = str(order_role or "").upper()
        if role == "PYRAMID":
            return "PENDING_PYRAMID"
        if role == "EXIT":
            return "PENDING_EXIT"
        if role == "ENTRY":
            return "PENDING_ENTRY"
        return "PENDING_ORDER"

    def _record_order_intent(
        self,
        instrument: Instrument,
        transaction_type: str,
        quantity: int,
        correlation_id: str,
        order_role: str,
        position_key: str,
        metadata: dict[str, Any] | None = None,
        attempt: int = 1,
    ) -> None:
        record = {
            "correlation_id": correlation_id,
            "order_id": "",
            "symbol": instrument.symbol,
            "security_id": str(instrument.security_id),
            "transaction_type": _broker_transaction_type({"transactionType": transaction_type}),
            "quantity": int(quantity),
            "order_role": order_role.upper(),
            "position_key": position_key,
            "status": self._ledger_order_status_for_role(order_role),
            "average_price": 0.0,
            "traded_quantity": 0,
            "attempt": attempt,
            "metadata": {"reference_price": float((metadata or {}).get("reference_price") or 0), **(metadata or {})},
            "applied_to_position": False,
            "created_at": datetime.now(IST).isoformat(),
            "updated_at": datetime.now(IST).isoformat(),
        }
        self.ledger.setdefault("orders", {})[correlation_id] = record
        self._save_ledger()

    def _update_ledger_order(self, correlation_id: str, updates: dict[str, Any]) -> None:
        if not correlation_id:
            return
        record = (self.ledger.setdefault("orders", {})).setdefault(correlation_id, {"correlation_id": correlation_id})
        record.update({key: value for key, value in updates.items() if value is not None})
        record["updated_at"] = datetime.now(IST).isoformat()
        self._save_ledger()

    def _mark_latest_order_applied(self, correlation_id: str) -> None:
        if correlation_id:
            record = (self.ledger.get("orders") or {}).get(correlation_id) or {}
            applied_quantity = int(record.get("traded_quantity") or record.get("quantity") or 0)
            self._update_ledger_order(correlation_id, {"applied_to_position": True, "applied_quantity": applied_quantity})

    def _mark_order_result_applied(self, order: dict[str, Any]) -> None:
        correlation_ids = order.get("correlation_ids") or [order.get("correlation_id")]
        for correlation_id in correlation_ids:
            self._mark_latest_order_applied(str(correlation_id or ""))

    def _update_ledger_order_from_broker(self, row: dict[str, Any], fallback_correlation_id: str = "") -> str:
        correlation_id = _broker_correlation_id(row) or fallback_correlation_id
        if not correlation_id:
            order_id = _broker_order_id(row)
            for key, record in (self.ledger.get("orders") or {}).items():
                if order_id and str(record.get("order_id") or "") == order_id:
                    correlation_id = key
                    break
        if not correlation_id:
            return ""
        status = _broker_order_status(row)
        average_price = self._average_price_from_order(row)
        traded_quantity = _broker_filled_quantity(row)
        requested_quantity = 0
        if correlation_id:
            requested_quantity = int(((self.ledger.get("orders") or {}).get(correlation_id) or {}).get("quantity") or 0)
        if traded_quantity > 0 and requested_quantity > 0 and traded_quantity < requested_quantity:
            status = "PARTIALLY_FILLED"
        broker_error = self._extract_error_message(row) if status in FAILED_STATUSES else ""
        self._update_ledger_order(
            correlation_id,
            {
                "order_id": _broker_order_id(row) or None,
                "status": status or None,
                "security_id": _broker_security_id(row) or None,
                "transaction_type": _broker_transaction_type(row) or None,
                "average_price": average_price or None,
                "traded_quantity": traded_quantity or None,
                "remaining_quantity": max(0, requested_quantity - traded_quantity) if requested_quantity else None,
                "error_message": broker_error or None,
                "raw": row,
            },
        )
        return correlation_id

    def _average_price_from_order(self, row: dict[str, Any]) -> float:
        return _broker_price(
            row,
            "averageTradedPrice",
            "AvgTradedPrice",
            "averagePrice",
            "avgPrice",
            "tradedPrice",
            "TradedPrice",
            "price",
            "buyAvg",
            "sellAvg",
        )

    def _is_symbol_locked(self, symbol: str) -> bool:
        self._refresh_external_worker_state()
        return self.entries_blocked_until_reconcile or symbol in self.locked_symbols or symbol in self.external_locked_symbols

    def _entry_block_reason(self, symbol: str) -> str:
        if self.entries_blocked_until_reconcile:
            return "broker reconciliation is pending"
        if symbol in self.locked_symbols or symbol in self.external_locked_symbols:
            return "broker/app quantity mismatch is locked"
        return ""

    def _refresh_external_worker_state(self) -> None:
        if self.enable_reconcile_workers:
            return
        now = time.monotonic()
        if now - self.last_external_state_load < 1:
            return
        self.last_external_state_load = now
        snapshot = read_json(RECONCILE_SNAPSHOT_FILE, {})
        if isinstance(snapshot, dict):
            self.external_locked_symbols = set(snapshot.get("locked_symbols") or [])
            broker = snapshot.get("broker_reconcile")
            if isinstance(broker, dict):
                self.broker_reconcile_status = broker
            reconcile = snapshot.get("reconcile")
            if isinstance(reconcile, dict):
                self.reconcile_status = reconcile
            premarket = snapshot.get("premarket")
            if isinstance(premarket, dict):
                self.premarket_status = premarket
        self._load_reconciled_candles()

    def _load_reconciled_candles(self) -> None:
        payload = read_json(RECONCILED_CANDLES_FILE, {})
        if not isinstance(payload, dict):
            return
        updated_at = str(payload.get("updated_at") or "")
        if not updated_at or updated_at == self.last_reconciled_candle_load:
            return
        instruments = self.instruments_by_symbol
        rows_by_symbol = payload.get("symbols") or {}
        for symbol, frames in rows_by_symbol.items():
            instrument = instruments.get(normalize_symbol(symbol))
            if not instrument or not isinstance(frames, dict):
                continue
            for timeframe in (1, 5):
                rows = frames.get(str(timeframe)) or []
                if rows:
                    self.candles.seed_history(instrument, timeframe, rows)
            self._mark_reconciled_day_history(instrument, frames.get("1") or [])
        self.last_reconciled_candle_load = updated_at

    def _extract_error_message(self, payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, str):
            return payload[:800]
        if isinstance(payload, dict):
            nested = payload.get("data") if isinstance(payload.get("data"), dict) else None
            candidates = [
                payload.get("errorMessage"),
                payload.get("error_message"),
                payload.get("message"),
                payload.get("remarks"),
                payload.get("rejectReason"),
                payload.get("rejectionReason"),
                payload.get("omsErrorDescription"),
                payload.get("omsErrorCode"),
                payload.get("ReasonDescription"),
                payload.get("reason"),
                payload.get("error"),
            ]
            if nested:
                candidates.extend(
                    [
                        nested.get("errorMessage"),
                        nested.get("message"),
                        nested.get("remarks"),
                        nested.get("rejectReason"),
                        nested.get("rejectionReason"),
                        nested.get("omsErrorDescription"),
                        nested.get("omsErrorCode"),
                        nested.get("ReasonDescription"),
                        nested.get("reason"),
                    ]
                )
            text = next((str(item) for item in candidates if item not in (None, "")), "")
            code = str(payload.get("errorCode") or (nested or {}).get("errorCode") or "")
            error_type = str(payload.get("errorType") or (nested or {}).get("errorType") or "")
            parts = [item for item in (code, error_type, text) if item]
            if parts:
                return " | ".join(parts)[:800]
            try:
                return json.dumps(payload, ensure_ascii=True)[:800]
            except TypeError:
                return str(payload)[:800]
        return str(payload)[:800]

    def _order_failure_reason(self, order: dict[str, Any]) -> str:
        return (
            str(order.get("error_message") or "")
            or self._extract_error_message(order.get("raw_detail"))
            or self._extract_error_message(order.get("raw"))
        )

    def _schedule_action(self, key: str, factory: Any, priority: int = 50) -> bool:
        if key in self.pending_actions:
            return False
        symbol = self._action_symbol(key)
        if not key.startswith("EXIT:") and any(self._action_symbol(pending) == symbol for pending in self.pending_actions):
            return False
        if self.execution_queue is None:
            self.execution_queue = asyncio.PriorityQueue()
        self.pending_actions.add(key)
        self.action_sequence += 1
        self.execution_queue.put_nowait((priority, self.action_sequence, key, factory))
        return True

    def _action_symbol(self, key: str) -> str:
        parts = key.split(":", 2)
        return parts[1] if len(parts) > 1 else key

    async def _execution_worker(self) -> None:
        assert self.execution_queue is not None
        while self.running:
            _priority, _sequence, key, factory = await self.execution_queue.get()
            try:
                symbol = self._action_symbol(key)
                lock = self.symbol_action_locks.get(symbol)
                if lock is None:
                    lock = asyncio.Lock()
                    self.symbol_action_locks[symbol] = lock
                async with lock:
                    await factory()
            except asyncio.CancelledError:
                raise
            except DhanAuthenticationError as exc:
                self._handle_auth_failure("Order action", exc)
            except Exception as exc:
                if _is_auth_error(exc):
                    self._handle_auth_failure("Order action", exc)
                    return
                self.event("ERROR", f"Order action failed: {exc}")
            finally:
                self.pending_actions.discard(key)
                self.execution_queue.task_done()

    def _handle_auth_failure(self, source: str, exc: Exception) -> None:
        self.last_error = f"{source}: Dhan credentials invalid or expired. Update client ID/access token and restart algo."
        self.event("ERROR", self.last_error)
        self.running = False
        self.feed_generation += 1
        self.entries_blocked_until_reconcile = True
        self.client = None
        self.market_connected = False
        self.market_connecting = False
        self.order_connected = False
        self.order_connecting = False
        self.reconcile_status = {
            "running": False,
            "message": self.last_error,
            "last_run": datetime.now(IST).isoformat(),
            "last_missing": {},
        }
        self.broker_reconcile_status = {
            **self.broker_reconcile_status,
            "running": False,
            "message": self.last_error,
            "last_run": datetime.now(IST).isoformat(),
            "entries_blocked_until_reconcile": True,
        }
        current = None
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in (self.tick_task, self.order_task, self.execution_task, self.snapshot_task, self.reconcile_task, self.broker_reconcile_task, *list(self.execution_tasks), *list(self.action_tasks)):
            if task and task is not current and not task.done():
                task.cancel()

    def configure(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            old_security_ids = set(self.instruments_by_security)
            if payload.get("credentials"):
                creds = payload["credentials"]
                client_id = str(creds.get("client_id") or "").strip()
                access_token = str(creds.get("access_token") or "").strip()
                if client_id and access_token:
                    self.credentials = {
                        "client_id": client_id,
                        "access_token": access_token,
                    }
                    self.client = DhanClient(self.credentials["client_id"], self.credentials["access_token"])
            if "settings" in payload:
                merged_settings = dict(self.settings.__dict__)
                current_enabled = dict(self.settings.enabled)
                incoming_settings = payload.get("settings") or {}
                incoming_enabled = incoming_settings.get("enabled")
                merged_settings.update(incoming_settings)
                if incoming_enabled is not None:
                    current_enabled.update(incoming_enabled)
                    merged_settings["enabled"] = current_enabled
                self.settings = StrategySettings.from_payload(merged_settings)
            if "universe_text" in payload:
                self.universe_symbols = set(extract_symbols(payload.get("universe_text") or ""))
            if "long_text" in payload:
                self.long_symbols = extract_symbols(payload.get("long_text") or "")
            if "short_text" in payload:
                self.short_symbols = extract_symbols(payload.get("short_text") or "")
            self._resolve_watchlists()
            if self.running and set(self.instruments_by_security) != old_security_ids:
                self._restart_market_feed_threads()
            self.save_state()
        return self.snapshot(fresh=True)

    def _resolve_watchlists(self) -> None:
        all_symbols = list(dict.fromkeys(self.long_symbols + self.short_symbols))
        self.instruments_by_symbol = {}
        self.instruments_by_security = {}
        if not all_symbols:
            return
        resolved, missing = self.resolver.resolve(all_symbols)
        for item in resolved:
            item.sector = ", ".join(sectors_for_symbol(item.symbol))
            self.instruments_by_symbol[item.symbol] = item
            self.instruments_by_security[str(item.security_id)] = item
        if missing:
            self.event("WARN", f"Missing Dhan security IDs: {', '.join(missing[:20])}")

    async def start(self) -> dict[str, Any]:
        if self.running:
            self.event("INFO", "Algo is already running.")
            return self.snapshot(fresh=True)
        self.event("INFO", "Start requested; running startup checks.")
        self.loop = asyncio.get_running_loop()
        self.tick_queue = asyncio.Queue(maxsize=TICK_QUEUE_MAXSIZE)
        self.execution_queue = asyncio.PriorityQueue()
        self.pending_actions = set()
        self.symbol_action_locks = {}
        self.execution_tasks = set()
        self.snapshot_cache = {}
        self.snapshot_cache_ts = 0.0
        if not self.client:
            if self.credentials.get("client_id") and self.credentials.get("access_token"):
                self.client = DhanClient(self.credentials["client_id"], self.credentials["access_token"])
            else:
                raise RuntimeError("Add Dhan client ID and access token first.")
        self._resolve_watchlists()
        if not self.instruments_by_symbol:
            raise RuntimeError("Add at least one resolved stock to the watchlists.")
        max_subscriptions = MAX_MARKET_FEED_CONNECTIONS * MAX_INSTRUMENTS_PER_CONNECTION
        subscription_count = len(self.instruments_by_symbol) + len(self.sector_instruments)
        if subscription_count > max_subscriptions:
            raise RuntimeError(f"Dhan supports {max_subscriptions} instruments across {MAX_MARKET_FEED_CONNECTIONS} feed connections.")
        if self.settings.dry_run:
            self.entries_blocked_until_reconcile = False
        else:
            self.entries_blocked_until_reconcile = False
            self.broker_reconcile_status = {
                **self.broker_reconcile_status,
                "running": False,
                "message": "Startup broker reconcile queued in background; trading continues and only mismatched/pending symbols will be locked.",
                "entries_blocked_until_reconcile": False,
            }
            self.event("INFO", self.broker_reconcile_status["message"])
        self.running = True
        self.scalper_state = {}
        self.reconciled_day_history = set()
        self.feed_generation += 1
        self.market_connecting = True
        self.order_connecting = True
        self.order_last_error = ""
        self.tick_task = asyncio.create_task(self._tick_worker())
        self.execution_tasks = {asyncio.create_task(self._execution_worker()) for _ in range(max(1, EXECUTION_WORKER_COUNT))}
        self.execution_task = next(iter(self.execution_tasks))
        if self.enable_snapshot_worker:
            self.snapshot_task = asyncio.create_task(self._snapshot_worker())
        self.order_task = asyncio.create_task(self._order_update_worker())
        if self.enable_reconcile_workers:
            self.reconcile_task = asyncio.create_task(self._auto_reconcile_worker())
            self.broker_reconcile_task = asyncio.create_task(self._broker_reconcile_worker())
        self._start_market_feed_threads(self.feed_generation)
        self.event("INFO", f"Algo started with {len(self.instruments_by_symbol)} stocks and {len(self.sector_instruments)} sector indexes on {self.settings.timeframe}m.")
        return self.snapshot(fresh=True)

    async def stop(self) -> dict[str, Any]:
        self.running = False
        self.feed_generation += 1
        self.market_connected = False
        self.market_connecting = False
        self.order_connected = False
        self.order_connecting = False
        self._close_market_feed_sockets()
        for task in (self.tick_task, self.order_task, self.execution_task, self.snapshot_task, self.reconcile_task, self.broker_reconcile_task, *list(self.execution_tasks), *list(self.action_tasks)):
            if task:
                task.cancel()
        self.execution_queue = None
        self.execution_tasks = set()
        self.execution_task = None
        self.symbol_action_locks = {}
        self.snapshot_task = None
        self.pending_actions.clear()
        self.event("INFO", "Algo stopped.")
        return self.snapshot(fresh=True)

    def _restart_market_feed_threads(self) -> None:
        max_subscriptions = MAX_MARKET_FEED_CONNECTIONS * MAX_INSTRUMENTS_PER_CONNECTION
        subscription_count = len(self.instruments_by_symbol) + len(self.sector_instruments)
        if subscription_count > max_subscriptions:
            self.event("ERROR", f"Market feed not restarted: Dhan supports {max_subscriptions} instruments across {MAX_MARKET_FEED_CONNECTIONS} feed connections.")
            return
        self.feed_generation += 1
        self.market_connected = False
        self.market_connecting = True
        self._close_market_feed_sockets()
        self._start_market_feed_threads(self.feed_generation)
        self.event("INFO", f"Market feed resubscribed with {len(self.instruments_by_symbol)} stocks and {len(self.sector_instruments)} sector indexes.")

    def _close_market_feed_sockets(self) -> None:
        with self.lock:
            sockets = list(self.feed_sockets)
            self.feed_sockets = []
        for ws in sockets:
            try:
                ws.close()
            except Exception:
                pass

    def _start_market_feed_threads(self, generation: int) -> None:
        try:
            import websocket
        except Exception as exc:
            self.last_error = f"Install websocket-client for Dhan market feed: {exc}"
            self.event("ERROR", self.last_error)
            return
        instruments = [
            {"ExchangeSegment": "NSE_EQ", "SecurityId": str(item.security_id)}
            for item in self.instruments_by_symbol.values()
        ]
        instruments.extend(
            {"ExchangeSegment": "IDX_I", "SecurityId": str(item.security_id)}
            for item in self.sector_instruments.values()
        )
        chunks = [instruments[i : i + MAX_INSTRUMENTS_PER_CONNECTION] for i in range(0, len(instruments), MAX_INSTRUMENTS_PER_CONNECTION)]
        for chunk in chunks[:MAX_MARKET_FEED_CONNECTIONS]:
            thread = threading.Thread(target=self._run_market_socket, args=(websocket, chunk, generation), daemon=True)
            thread.start()
            self.feed_threads.append(thread)

    def _run_market_socket(self, websocket_module: Any, instruments: list[dict[str, str]], generation: int) -> None:
        url = DHAN_FEED_URL.format(token=self.credentials.get("access_token"), client_id=self.credentials.get("client_id"))

        def on_open(ws):
            self.market_connected = True
            self.market_connecting = False
            self.last_error = ""
            self.event("INFO", f"Dhan market WebSocket connected with {len(instruments)} instruments.")
            for batch in chunked(instruments, MAX_SUBSCRIBE_BATCH):
                ws.send(json.dumps({"RequestCode": 17, "InstrumentCount": len(batch), "InstrumentList": batch}))

        def on_message(_ws, message):
            if generation != self.feed_generation:
                return
            if isinstance(message, bytes):
                self._on_binary_tick(message)
            elif isinstance(message, str) and "error" in message.lower():
                self.last_error = message[:250]
                if "DH-901" in message or "Invalid_Authentication" in message:
                    self._handle_auth_failure("Dhan market WebSocket", DhanAuthenticationError(message[:300]))

        def on_error(_ws, error):
            self.market_connected = False
            self.market_connecting = False
            self.last_error = f"Dhan market WebSocket error: {error}"
            self.event("WARN", self.last_error)

        def on_close(_ws, code, reason):
            self.market_connected = False
            self.market_connecting = False
            if generation == self.feed_generation and self.running:
                self.last_error = f"Dhan market WebSocket closed: {code or ''} {reason or ''}".strip()
                self.event("WARN", self.last_error)

        backoff = 2
        while generation == self.feed_generation and self.running:
            self.market_connecting = True
            ws = websocket_module.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            with self.lock:
                self.feed_sockets.append(ws)
            try:
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self.market_connected = False
                self.market_connecting = False
                self.last_error = f"Dhan market WebSocket stopped: {exc}"
                self.event("WARN", self.last_error)
            finally:
                with self.lock:
                    if ws in self.feed_sockets:
                        self.feed_sockets.remove(ws)
            if generation != self.feed_generation or not self.running:
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _on_binary_tick(self, message: bytes) -> None:
        if len(message) < 12 or not self.loop or not self.tick_queue:
            return
        try:
            packet_code, _length, exchange_code, security_id = struct.unpack_from("<B H B I", message, 0)
            segment = EXCHANGE_SEGMENT_CODES.get(exchange_code)
            if segment not in {"NSE_EQ", "IDX_I"} or packet_code not in {2, 4, 8}:
                return
            price = struct.unpack_from("<f", message, 8)[0]
            volume = None
            if packet_code in {4, 8} and len(message) >= 26:
                volume = struct.unpack_from("<I", message, 22)[0]
            self.last_tick_ts = time.time()
            self.loop.call_soon_threadsafe(self._enqueue_tick, segment, str(security_id), float(price), volume, datetime.now(IST))
        except Exception as exc:
            self.last_error = f"Dhan tick parse failed: {exc}"

    def _enqueue_tick(self, segment: str, security_id: str, price: float, volume: int | None, timestamp: datetime) -> None:
        if not self.tick_queue:
            return
        item = (segment, security_id, price, volume, timestamp)
        try:
            self.tick_queue.put_nowait(item)
        except asyncio.QueueFull:
            self.dropped_ticks += 1
            try:
                self.tick_queue.get_nowait()
                self.tick_queue.put_nowait(item)
            except asyncio.QueueFull:
                pass
            except asyncio.QueueEmpty:
                pass
            now = time.monotonic()
            if now - self.last_tick_queue_warn_ts >= TICK_QUEUE_WARN_INTERVAL_SECONDS:
                self.last_tick_queue_warn_ts = now
                self.event(
                    "WARN",
                    f"Tick queue pressure: dropped oldest ticks {self.dropped_ticks}, queue size {self.tick_queue.qsize()}/{self.tick_queue.maxsize}.",
                )

    async def _tick_worker(self) -> None:
        assert self.tick_queue is not None
        while self.running:
            segment, security_id, price, volume, timestamp = await self.tick_queue.get()
            if segment == "IDX_I":
                self._on_sector_tick(security_id, price, timestamp)
                continue
            instrument = self.instruments_by_security.get(str(security_id))
            if not instrument:
                continue
            closed = self.candles.on_tick(instrument, price, volume, timestamp)
            await self._evaluate_tick(instrument, price, timestamp)
            for candle in closed:
                await self._evaluate_closed_candle(instrument, candle)

    async def _evaluate_tick(self, instrument: Instrument, price: float, timestamp: datetime | None = None) -> None:
        symbol = instrument.symbol
        timestamp = to_ist(timestamp)
        for position in self._open_positions_for_symbol(symbol):
            position.last_price = price
            if position.strategy in {"scalper_long", "scalper_short"}:
                self._schedule_scalper_management(position, instrument, price)
        await self._evaluate_scalper_tick(instrument, price, timestamp)
        if self._is_symbol_locked(symbol):
            return
        if symbol not in self.long_symbols or self._has_open_position(symbol):
            return
        if not self._passes_sector_filter(symbol, "long"):
            return
        candles = self.candles.all_candles(symbol, self.settings.timeframe)
        signal = self.long_evaluator.evaluate_tick_entry(symbol, price, candles, self.settings)
        if signal:
            self._schedule_action(
                f"ENTRY:{instrument.symbol}:LONG",
                lambda instrument=instrument, signal=signal: self._enter_position(instrument, signal),
                priority=50,
            )

    async def _evaluate_closed_candle(self, instrument: Instrument, candle: Candle) -> None:
        if candle.timeframe != self.settings.timeframe:
            return
        symbol = instrument.symbol
        candles = self.candles.closed_candles(symbol, self.settings.timeframe)
        for position in list(self.positions.values()):
            if position.symbol == symbol and position.status == "OPEN":
                if position.strategy in {"scalper_long", "scalper_short"}:
                    continue
                reason = self.long_evaluator.evaluate_exit(position, candles, self.settings)
                if reason:
                    self._schedule_action(
                        f"EXIT:{position.symbol}:{position.side}",
                        lambda position=position, reason=reason: self._exit_position(position, reason),
                        priority=0,
                    )
        if symbol not in self.long_symbols or self._has_open_position(symbol):
            return
        if self._is_symbol_locked(symbol):
            return
        previous_day = self._previous_day(symbol)
        baseline = self._baseline(symbol, self.settings.timeframe)
        for event in self.long_evaluator.on_closed_candle(symbol, candles, previous_day, baseline, self.settings):
            self.event(event.get("type", "INFO"), f"{symbol} {event.get('strategy')}: {event.get('reason')}")
            if event.get("type") == "ENTRY" and self._passes_sector_filter(symbol, "long"):
                self._schedule_action(
                    f"ENTRY:{instrument.symbol}:LONG",
                    lambda instrument=instrument, event=event: self._enter_position(instrument, event),
                    priority=50,
                )

    async def _enter_position(self, instrument: Instrument, signal: dict[str, Any]) -> None:
        if self._is_symbol_locked(instrument.symbol):
            self.event("WARN", f"{instrument.symbol} entry blocked: {self._entry_block_reason(instrument.symbol)}.")
            return
        if self._has_open_position(instrument.symbol):
            self.event("WARN", f"{instrument.symbol} entry skipped: an app position is already open.")
            return
        entry = float(signal["entry_price"])
        stop, target = setup_stop(entry, signal["stop_candle"], self.settings)
        quantity, sizing_reason = calculate_quantity(entry, stop, self.settings, "LONG")
        if quantity < 1:
            self.event("WARN", f"{instrument.symbol} entry skipped: calculated quantity is 0 ({sizing_reason}).")
            return
        correlation_id = f"KJ{uuid4().hex[:18]}"
        position_key = self._position_key(instrument.symbol, "LONG")
        order = await self._place_with_retry(
            instrument,
            "BUY",
            quantity,
            correlation_id,
            order_role="ENTRY",
            position_key=position_key,
            reference_price=entry,
            metadata={
                "position": {
                    "side": "LONG",
                    "strategy": signal["strategy"],
                    "entry_price": entry,
                    "stop_loss": stop,
                    "target": target,
                    "quantity": quantity,
                    "reason": signal.get("reason"),
                    "sizing": sizing_reason,
                }
            },
        )
        status = order.get("status", "")
        if str(status).upper() not in FILL_ACCEPTED_STATUSES:
            reason = self._order_failure_reason(order)
            suffix = f": {reason}" if reason else "."
            self.event("ERROR", f"{instrument.symbol} entry not opened because order status is {status or 'UNKNOWN'}{suffix}")
            return
        executed_quantity = int(order.get("traded_quantity") or quantity)
        if executed_quantity < 1:
            self.event("ERROR", f"{instrument.symbol} entry not opened because broker traded quantity is 0.")
            return
        position = Position(
            symbol=instrument.symbol,
            security_id=instrument.security_id,
            side="LONG",
            strategy=signal["strategy"],
            entry_price=entry,
            quantity=executed_quantity,
            stop_loss=stop,
            target=target,
            opened_at=datetime.now(IST).isoformat(),
            order_id=str(order.get("order_id") or ""),
            last_price=entry,
            meta={"entry_order_status": status, "reason": signal.get("reason"), "sizing": sizing_reason, "requested_quantity": quantity},
        )
        self.positions[position_key] = position
        self._mark_order_result_applied(order)
        self._persist_position(position)
        self.event("ENTRY", f"{instrument.symbol} {signal['strategy']} BUY {executed_quantity} at {entry:.2f}, SL {stop:.2f}, target {target:.2f} ({sizing_reason})")

    async def _evaluate_scalper_tick(self, instrument: Instrument, price: float, timestamp: datetime) -> None:
        if price <= 0 or self._has_open_position(instrument.symbol):
            return
        if self._is_symbol_locked(instrument.symbol):
            return
        want_long = self.settings.enabled.get("scalper_long") and instrument.symbol in self.long_symbols
        want_short = self.settings.enabled.get("scalper_short") and instrument.symbol in self.short_symbols
        if not want_long and not want_short:
            return
        state = self._scalper_reference_state(instrument.symbol, price, timestamp)
        if not state.get("ready"):
            return
        if want_long and not state.get("long_triggered"):
            if self._passes_sector_filter(instrument.symbol, "long") and price > float(state.get("high") or 0):
                scheduled = self._schedule_action(
                    f"ENTRY:{instrument.symbol}:LONG",
                    lambda instrument=instrument, price=price: self._enter_scalper_position(instrument, "LONG", price, "Break above scalper reference high"),
                    priority=50,
                )
                if scheduled:
                    state["long_triggered"] = True
                    return
        if want_short and not state.get("short_triggered"):
            if self._passes_sector_filter(instrument.symbol, "short") and price < float(state.get("low") or 0):
                scheduled = self._schedule_action(
                    f"ENTRY:{instrument.symbol}:SHORT",
                    lambda instrument=instrument, price=price: self._enter_scalper_position(instrument, "SHORT", price, "Break below scalper reference low"),
                    priority=50,
                )
                if scheduled:
                    state["short_triggered"] = True

    def _scalper_reference_state(self, symbol: str, price: float, timestamp: datetime) -> dict[str, Any]:
        timestamp = to_ist(timestamp)
        session_open_dt = timestamp.replace(
            hour=SESSION_OPEN.hour,
            minute=SESSION_OPEN.minute,
            second=0,
            microsecond=0,
        )
        range_end = session_open_dt + timedelta(seconds=5)
        state = self.scalper_state.get(symbol)
        if state and state.get("session_date") != timestamp.date().isoformat():
            state = None
        if not state:
            state = {
                "session_date": timestamp.date().isoformat(),
                "high": 0.0,
                "low": 0.0,
                "ready": False,
                "ready_logged": False,
                "long_triggered": False,
                "short_triggered": False,
                "mode": "opening_range",
            }
            self.scalper_state[symbol] = state
        if timestamp <= range_end:
            state["high"] = max(float(state.get("high") or 0), price)
            current_low = float(state.get("low") or 0)
            state["low"] = min(current_low, price) if current_low > 0 else price
            state["ready"] = False
            return state
        if not state.get("ready"):
            high = float(state.get("high") or 0)
            low = float(state.get("low") or 0)
            if high <= 0 or low <= 0:
                if not self._has_opening_history(symbol, timestamp):
                    if state.get("mode") != "awaiting_day_history":
                        self.event("INFO", f"{symbol} scalper waiting for reconciled day-high/day-low history.")
                    state["mode"] = "awaiting_day_history"
                    return state
                high, low = self._day_extremes_until(symbol, price, timestamp)
                state["mode"] = "late_day_reference"
            state["high"] = high
            state["low"] = low
            state["ready"] = True
            state["ready_at"] = timestamp.isoformat()
            if not state.get("ready_logged"):
                self.event("INFO", f"{symbol} scalper ready | high {high:.2f} | low {low:.2f} | mode {state.get('mode') or 'opening_range'}")
                state["ready_logged"] = True
            return state
        return state

    def _has_opening_history(self, symbol: str, timestamp: datetime) -> bool:
        return symbol in self.reconciled_day_history

    def _mark_reconciled_day_history(self, instrument: Instrument, rows: list[dict[str, Any]], as_of: datetime | None = None) -> None:
        now = as_of or datetime.now(IST)
        session_open_dt = now.replace(
            hour=SESSION_OPEN.hour,
            minute=SESSION_OPEN.minute,
            second=0,
            microsecond=0,
        )
        for row in rows:
            timestamp = row.get("timestamp") or row.get("start") or row.get("time")
            if timestamp and to_ist(timestamp).replace(second=0, microsecond=0) == session_open_dt:
                self.reconciled_day_history.add(instrument.symbol)
                return
        one_minute_open_label = session_open_dt + timedelta(minutes=1)
        one_minute_has_opening = False
        for candle in self.candles.closed_candles(instrument.symbol, 1):
            start = to_ist(candle.start)
            if start.date() == now.date() and session_open_dt <= start <= one_minute_open_label:
                one_minute_has_opening = True
                break
        five_minute_has_opening = any(
            to_ist(candle.start).date() == now.date() and to_ist(candle.start) == session_open_dt
            for candle in self.candles.closed_candles(instrument.symbol, 5)
        )
        if one_minute_has_opening and five_minute_has_opening:
            self.reconciled_day_history.add(instrument.symbol)

    def _day_extremes_until(self, symbol: str, price: float, timestamp: datetime) -> tuple[float, float]:
        session_open_dt = timestamp.replace(
            hour=SESSION_OPEN.hour,
            minute=SESSION_OPEN.minute,
            second=0,
            microsecond=0,
        )
        highs = [float(price)]
        lows = [float(price)]
        for candle in self.candles.all_candles(symbol, 1):
            start = to_ist(candle.start)
            if start.date() == timestamp.date() and session_open_dt <= start <= timestamp:
                highs.append(float(candle.high))
                lows.append(float(candle.low))
        return max(highs), min(lows)

    async def _enter_scalper_position(self, instrument: Instrument, side: str, entry: float, reason: str) -> bool:
        if self._is_symbol_locked(instrument.symbol):
            self.event("WARN", f"{instrument.symbol} scalper {side.lower()} blocked: {self._entry_block_reason(instrument.symbol)}.")
            return False
        if self._has_open_position(instrument.symbol):
            self.event("WARN", f"{instrument.symbol} scalper {side.lower()} skipped: an app position is already open.")
            return False
        stop, target = self._scalper_stop_target(entry, side)
        quantity, sizing_reason = calculate_quantity(entry, stop, self.settings, side)
        if quantity < 1:
            self.event("WARN", f"{instrument.symbol} scalper {side.lower()} skipped: calculated quantity is 0 ({sizing_reason}).")
            return False
        transaction_type = "BUY" if side == "LONG" else "SELL"
        correlation_id = f"KJS{uuid4().hex[:17]}"
        position_key = self._position_key(instrument.symbol, side)
        strategy = "scalper_long" if side == "LONG" else "scalper_short"
        order = await self._place_with_retry(
            instrument,
            transaction_type,
            quantity,
            correlation_id,
            order_role="ENTRY",
            position_key=position_key,
            reference_price=entry,
            metadata={
                "position": {
                    "side": side,
                    "strategy": strategy,
                    "entry_price": entry,
                    "stop_loss": stop,
                    "target": target,
                    "quantity": quantity,
                    "reason": reason,
                    "sizing": sizing_reason,
                }
            },
        )
        status = str(order.get("status") or "").upper()
        if status not in FILL_ACCEPTED_STATUSES:
            reason = self._order_failure_reason(order)
            suffix = f": {reason}" if reason else "."
            self.event("ERROR", f"{instrument.symbol} scalper {side.lower()} not opened because order status is {status or 'UNKNOWN'}{suffix}")
            return False
        executed_quantity = int(order.get("traded_quantity") or quantity)
        if executed_quantity < 1:
            self.event("ERROR", f"{instrument.symbol} scalper {side.lower()} not opened because broker traded quantity is 0.")
            return False
        initial_risk = abs(entry - stop)
        position = Position(
            symbol=instrument.symbol,
            security_id=instrument.security_id,
            side=side,
            strategy=strategy,
            entry_price=round(entry, 2),
            quantity=executed_quantity,
            stop_loss=stop,
            target=target,
            opened_at=datetime.now(IST).isoformat(),
            order_id=str(order.get("order_id") or ""),
            last_price=entry,
            meta={
                "entry_order_status": status,
                "reason": reason,
                "sizing": sizing_reason,
                "initial_entry": entry,
                "initial_risk": initial_risk,
                "initial_quantity": executed_quantity,
                "requested_quantity": quantity,
                "pyramid_adds": 0,
                "pyramid_orders": [],
                "breakeven_moved": False,
                "sl_percent": self.settings.scalper_sl_percent,
            },
        )
        self.positions[position_key] = position
        self._mark_order_result_applied(order)
        self._persist_position(position)
        self.event("ENTRY", f"{instrument.symbol} {strategy} {transaction_type} {executed_quantity} at {entry:.2f}, SL {stop:.2f}, target {target:.2f} ({sizing_reason})")
        return True

    def _scalper_stop_target(self, entry: float, side: str) -> tuple[float, float]:
        sl_percent = max(0.1, float(self.settings.scalper_sl_percent or 0.8))
        risk = entry * sl_percent / 100
        if side == "SHORT":
            stop = entry + risk
            target = entry - risk * self.settings.risk_reward
        else:
            stop = entry - risk
            target = entry + risk * self.settings.risk_reward
        return round(stop, 2), round(target, 2)

    async def _manage_scalper_position(self, position: Position, instrument: Instrument, price: float) -> None:
        if position.status != "OPEN":
            return
        reason = self._scalper_exit_reason(position, price)
        if reason:
            await self._exit_position(position, reason)
            return
        self._trail_scalper_to_breakeven(position, price)
        await self._maybe_pyramid_scalper(position, instrument, price)

    def _schedule_scalper_management(self, position: Position, instrument: Instrument, price: float) -> None:
        if position.status != "OPEN":
            return
        reason = self._scalper_exit_reason(position, price)
        if reason:
            self._schedule_action(
                f"EXIT:{position.symbol}:{position.side}",
                lambda position=position, reason=reason: self._exit_position(position, reason),
                priority=0,
            )
            return
        self._trail_scalper_to_breakeven(position, price)
        if self._scalper_pyramid_due(position, price):
            self._schedule_action(
                f"PYRAMID:{position.symbol}:{position.side}",
                lambda position=position, instrument=instrument, price=price: self._maybe_pyramid_scalper(position, instrument, price),
                priority=20,
            )

    def _scalper_exit_reason(self, position: Position, price: float) -> str:
        if position.side == "SHORT":
            if price >= position.stop_loss:
                return "STOP_LOSS"
            if price <= position.target:
                return "TARGET"
            return ""
        if price <= position.stop_loss:
            return "STOP_LOSS"
        if price >= position.target:
            return "TARGET"
        return ""

    def _scalper_pyramid_due(self, position: Position, price: float) -> bool:
        if not self.settings.scalper_pyramiding:
            return False
        max_adds = max(0, int(self.settings.scalper_max_adds or 0))
        add_count = int(position.meta.get("pyramid_adds") or 0)
        initial_entry = float(position.meta.get("initial_entry") or position.entry_price)
        sl_percent = max(0.1, float(position.meta.get("sl_percent") or self.settings.scalper_sl_percent or 0.8))
        if add_count >= max_adds or initial_entry <= 0:
            return False
        next_add = add_count + 1
        if position.side == "SHORT":
            return price <= initial_entry * (1 - (sl_percent / 100) * next_add)
        return price >= initial_entry * (1 + (sl_percent / 100) * next_add)

    def _trail_scalper_to_breakeven(self, position: Position, price: float) -> None:
        if position.meta.get("breakeven_moved"):
            return
        initial_entry = float(position.meta.get("initial_entry") or position.entry_price)
        initial_risk = float(position.meta.get("initial_risk") or abs(position.entry_price - position.stop_loss))
        if initial_risk <= 0:
            return
        if position.side == "SHORT":
            trigger = initial_entry - initial_risk * 2
            if price <= trigger:
                position.stop_loss = min(position.stop_loss, round(position.entry_price, 2))
                position.meta["breakeven_moved"] = True
                self._persist_position(position)
                self.event("INFO", f"{position.symbol} scalper short SL trailed to cost {position.stop_loss:.2f}.")
            return
        trigger = initial_entry + initial_risk * 2
        if price >= trigger:
            position.stop_loss = max(position.stop_loss, round(position.entry_price, 2))
            position.meta["breakeven_moved"] = True
            self._persist_position(position)
            self.event("INFO", f"{position.symbol} scalper long SL trailed to cost {position.stop_loss:.2f}.")

    async def _maybe_pyramid_scalper(self, position: Position, instrument: Instrument, price: float) -> None:
        if not self.settings.scalper_pyramiding:
            return
        if position.status != "OPEN":
            return
        if self._is_symbol_locked(position.symbol):
            self.event("WARN", f"{position.symbol} scalper pyramid blocked: {self._entry_block_reason(position.symbol)}.")
            return
        max_adds = max(0, int(self.settings.scalper_max_adds or 0))
        add_count = int(position.meta.get("pyramid_adds") or 0)
        initial_quantity = int(position.meta.get("initial_quantity") or position.quantity)
        initial_entry = float(position.meta.get("initial_entry") or position.entry_price)
        sl_percent = max(0.1, float(position.meta.get("sl_percent") or self.settings.scalper_sl_percent or 0.8))
        if max_adds <= 0 or initial_quantity <= 0 or initial_entry <= 0:
            return
        while add_count < max_adds:
            next_add = add_count + 1
            if position.side == "SHORT":
                threshold = initial_entry * (1 - (sl_percent / 100) * next_add)
                should_add = price <= threshold
                transaction_type = "SELL"
            else:
                threshold = initial_entry * (1 + (sl_percent / 100) * next_add)
                should_add = price >= threshold
                transaction_type = "BUY"
            if not should_add:
                return
            order = await self._place_with_retry(
                instrument,
                transaction_type,
                initial_quantity,
                f"KJP{uuid4().hex[:17]}",
                order_role="PYRAMID",
                position_key=self._position_key(position.symbol, position.side),
                reference_price=price,
                metadata={"pyramid_add": next_add, "initial_quantity": initial_quantity},
            )
            status = str(order.get("status") or "").upper()
            if status not in FILL_ACCEPTED_STATUSES:
                reason = self._order_failure_reason(order)
                suffix = f": {reason}" if reason else ""
                self.event("ERROR", f"{position.symbol} scalper pyramid add {next_add} failed with status {status or 'UNKNOWN'}{suffix}; algo quantity unchanged.")
                return
            executed_quantity = int(order.get("traded_quantity") or initial_quantity)
            if executed_quantity < 1:
                self.event("ERROR", f"{position.symbol} scalper pyramid add {next_add} returned traded quantity 0; algo quantity unchanged.")
                return
            old_quantity = position.quantity
            new_quantity = old_quantity + executed_quantity
            fill_price = float(order.get("average_price") or price)
            position.entry_price = round(((position.entry_price * old_quantity) + (fill_price * executed_quantity)) / new_quantity, 2)
            position.quantity = new_quantity
            add_count = next_add
            position.meta["pyramid_adds"] = add_count
            position.meta.setdefault("pyramid_orders", []).append(
                {"order_id": str(order.get("order_id") or ""), "quantity": executed_quantity, "requested_quantity": initial_quantity, "price": fill_price, "status": status}
            )
            self._refresh_scalper_risk(position)
            self._mark_order_result_applied(order)
            self._persist_position(position)
            self.event("ENTRY", f"{position.symbol} scalper pyramid {transaction_type} add {add_count}/{max_adds}: +{executed_quantity}, total {position.quantity}, avg {position.entry_price:.2f}.")

    def _refresh_scalper_risk(self, position: Position) -> None:
        sl_percent = max(0.1, float(position.meta.get("sl_percent") or self.settings.scalper_sl_percent or 0.8))
        risk = position.entry_price * sl_percent / 100
        if position.side == "SHORT":
            stop = position.entry_price + risk
            target = position.entry_price - risk * self.settings.risk_reward
            position.stop_loss = round(position.entry_price if position.meta.get("breakeven_moved") else stop, 2)
            position.target = round(target, 2)
            return
        stop = position.entry_price - risk
        target = position.entry_price + risk * self.settings.risk_reward
        position.stop_loss = round(position.entry_price if position.meta.get("breakeven_moved") else stop, 2)
        position.target = round(target, 2)

    async def _exit_position(self, position: Position, reason: str) -> None:
        if position.status != "OPEN":
            return
        transaction_type = "BUY" if position.side == "SHORT" else "SELL"
        order = await self._place_with_retry(
            Instrument(position.symbol, position.security_id),
            transaction_type,
            position.quantity,
            f"KJX{uuid4().hex[:17]}",
            order_role="EXIT",
            position_key=self._position_key(position.symbol, position.side),
            reference_price=position.last_price,
            metadata={"reason": reason},
        )
        status = str(order.get("status") or "").upper()
        if status not in FILL_ACCEPTED_STATUSES:
            failure = self._order_failure_reason(order)
            suffix = f": {failure}" if failure else "."
            self.event("ERROR", f"{position.symbol} exit {reason} not marked closed because order status is {status or 'UNKNOWN'}{suffix}")
            return
        executed_quantity = int(order.get("traded_quantity") or position.quantity)
        if executed_quantity < position.quantity:
            position.quantity -= max(0, executed_quantity)
            position.exit_order_id = str(order.get("order_id") or "")
            position.exit_reason = f"PARTIAL_{reason}"
            self._mark_order_result_applied(order)
            self._persist_position(position)
            self.event("WARN", f"{position.symbol} partial exit {reason}: exited {executed_quantity}, remaining {position.quantity}.")
            return
        position.status = "CLOSED"
        position.exit_order_id = str(order.get("order_id") or "")
        position.exit_reason = reason
        self._mark_order_result_applied(order)
        self._persist_position(position)
        self.event("EXIT", f"{position.symbol} exit {reason} at approx {position.last_price or 0:.2f}")

    async def _place_with_retry(
        self,
        instrument: Instrument,
        transaction_type: str,
        quantity: int,
        correlation_id: str,
        order_role: str = "ORDER",
        position_key: str = "",
        reference_price: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        intent_metadata = dict(metadata or {})
        intent_metadata.setdefault("reference_price", float(reference_price or 0))
        if self.settings.dry_run:
            dry_correlation_id = f"{correlation_id}1"[:30]
            self._record_order_intent(
                instrument,
                transaction_type,
                quantity,
                dry_correlation_id,
                order_role,
                position_key,
                metadata=intent_metadata,
                attempt=1,
            )
            order_id = f"DRY-{uuid4().hex[:10]}"
            self._update_ledger_order(
                dry_correlation_id,
                {
                    "order_id": order_id,
                    "status": "TRADED",
                    "average_price": float(reference_price or 0),
                    "traded_quantity": int(quantity),
                    "raw": {"dry_run": True},
                },
            )
            return {
                "order_id": order_id,
                "status": "TRADED",
                "attempts": 1,
                "correlation_id": dry_correlation_id,
                "average_price": float(reference_price or 0),
                "traded_quantity": int(quantity),
            }
        assert self.client is not None
        last_result: dict[str, Any] = {}
        requested_total = int(quantity)
        remaining_quantity = requested_total
        total_traded = 0
        total_value = 0.0
        filled_correlations: list[str] = []
        order_ids: list[str] = []
        for attempt in range(1, 4):
            if remaining_quantity <= 0:
                break
            attempt_quantity = remaining_quantity
            attempt_correlation_id = f"{correlation_id}{attempt}"[:30]
            self._record_order_intent(
                instrument,
                transaction_type,
                attempt_quantity,
                attempt_correlation_id,
                order_role,
                position_key,
                metadata=intent_metadata,
                attempt=attempt,
            )
            try:
                result = await self.client.place_market_order(instrument.security_id, transaction_type, attempt_quantity, attempt_correlation_id)
            except Exception as exc:
                request_payload = getattr(exc, "request_payload", {})
                payload_note = ""
                if isinstance(request_payload, dict) and request_payload:
                    payload_note = (
                        f" | payload securityId={request_payload.get('securityId')}, "
                        f"transactionType={request_payload.get('transactionType')}, "
                        f"quantity={request_payload.get('quantity')}, "
                        f"productType={request_payload.get('productType')}, "
                        f"orderType={request_payload.get('orderType')}"
                    )
                last_result = {
                    "order_id": "",
                    "status": "PLACE_FAILED",
                    "attempts": attempt,
                    "raw": {"error": str(exc)[:500], "request_payload": request_payload or None},
                    "correlation_id": attempt_correlation_id,
                    "average_price": 0.0,
                    "traded_quantity": 0,
                    "error_message": (str(exc) + payload_note)[:1000],
                }
                self._update_ledger_order(
                    attempt_correlation_id,
                    {
                        "status": "PLACE_FAILED",
                        "raw": last_result["raw"],
                        "error_message": last_result["error_message"],
                    },
                )
                if isinstance(exc, DhanAuthenticationError) or _is_auth_error(exc):
                    raise
                await asyncio.sleep(0.25 * attempt)
                continue
            order_id = str(result.get("orderId") or (result.get("data") or {}).get("orderId") or "")
            if order_id:
                order_ids.append(order_id)
            status = str(result.get("orderStatus") or (result.get("data") or {}).get("orderStatus") or "").upper()
            last_result = {
                "order_id": order_id,
                "order_ids": order_ids[:],
                "status": status,
                "attempts": attempt,
                "raw": result,
                "correlation_id": attempt_correlation_id,
                "correlation_ids": filled_correlations[:],
                "average_price": 0.0,
                "traded_quantity": 0,
                "requested_quantity": requested_total,
                "remaining_quantity": remaining_quantity,
            }
            self._update_ledger_order(attempt_correlation_id, {"order_id": order_id, "status": status, "raw": result})
            final_status = await self._wait_order_status(order_id, status)
            last_result["status"] = final_status
            details = await self._order_fill_details(order_id, reference_price if final_status in FILL_ACCEPTED_STATUSES else 0)
            last_result.update(details)
            traded_quantity = int(details.get("traded_quantity") or 0)
            if final_status in FILL_ACCEPTED_STATUSES and traded_quantity <= 0:
                last_result["status"] = "TRADED_UNCONFIRMED"
                last_result["error_message"] = "Broker reported fill status but traded quantity could not be confirmed; retry stopped to avoid duplicate quantity."
                self._update_ledger_order(
                    attempt_correlation_id,
                    {
                        "status": "TRADED_UNCONFIRMED",
                        "error_message": last_result["error_message"],
                    },
                )
                return last_result
            if traded_quantity > 0:
                fill_price = float(details.get("average_price") or reference_price or 0)
                total_traded += traded_quantity
                if fill_price > 0:
                    total_value += fill_price * traded_quantity
                filled_correlations.append(attempt_correlation_id)
                remaining_quantity = max(0, requested_total - total_traded)
            if traded_quantity > 0 and traded_quantity < int(attempt_quantity):
                final_status = "PARTIALLY_FILLED"
                last_result["status"] = final_status
            elif traded_quantity >= int(attempt_quantity) and final_status in PARTIAL_FILL_STATUSES:
                final_status = "TRADED"
                last_result["status"] = final_status
            if final_status not in FILL_ACCEPTED_STATUSES:
                detail = await self._order_detail(order_id)
                if detail:
                    last_result["raw_detail"] = detail
                    last_result["error_message"] = self._extract_error_message(detail)
                    self._update_ledger_order(attempt_correlation_id, {"raw_detail": detail, "error_message": last_result["error_message"]})
            self._update_ledger_order(
                attempt_correlation_id,
                {
                    "status": final_status,
                    "average_price": details.get("average_price") or None,
                    "traded_quantity": details.get("traded_quantity") or None,
                    "remaining_quantity": max(0, attempt_quantity - traded_quantity),
                    "error_message": last_result.get("error_message") or None,
                },
            )
            if total_traded >= requested_total:
                aggregate_price = round(total_value / total_traded, 4) if total_traded > 0 and total_value > 0 else float(reference_price or 0)
                return {
                    **last_result,
                    "order_id": order_ids[-1] if order_ids else order_id,
                    "order_ids": order_ids[:],
                    "status": "TRADED",
                    "attempts": attempt,
                    "correlation_id": filled_correlations[-1] if filled_correlations else attempt_correlation_id,
                    "correlation_ids": filled_correlations[:],
                    "average_price": aggregate_price,
                    "traded_quantity": total_traded,
                    "requested_quantity": requested_total,
                    "remaining_quantity": 0,
                }
            if final_status in FILL_ACCEPTED_STATUSES and traded_quantity > 0:
                self.event("WARN", f"{instrument.symbol} {transaction_type} partially filled {total_traded}/{requested_total}; retrying remaining {remaining_quantity}.")
            if order_id and (final_status in PENDING_STATUSES or final_status in PARTIAL_FILL_STATUSES):
                try:
                    await self.client.cancel_order(order_id)
                    self._update_ledger_order(attempt_correlation_id, {"status": "CANCEL_REQUESTED"})
                except Exception as exc:
                    if isinstance(exc, DhanAuthenticationError) or _is_auth_error(exc):
                        raise
                    refreshed = await self._refresh_after_cancel_reject(
                        instrument,
                        order_id,
                        attempt_correlation_id,
                        final_status,
                        reference_price,
                        exc,
                    )
                    last_result.update(refreshed)
                    refreshed_status = str(refreshed.get("status") or "").upper()
                    refreshed_traded = int(refreshed.get("traded_quantity") or 0)
                    additional_traded = max(0, refreshed_traded - traded_quantity)
                    if additional_traded > 0:
                        fill_price = float(refreshed.get("average_price") or reference_price or 0)
                        total_traded += additional_traded
                        if fill_price > 0:
                            total_value += fill_price * additional_traded
                        if attempt_correlation_id not in filled_correlations:
                            filled_correlations.append(attempt_correlation_id)
                        remaining_quantity = max(0, requested_total - total_traded)
                    if total_traded >= requested_total:
                        aggregate_price = round(total_value / total_traded, 4) if total_traded > 0 and total_value > 0 else float(reference_price or 0)
                        return {
                            **last_result,
                            "order_id": order_ids[-1] if order_ids else order_id,
                            "order_ids": order_ids[:],
                            "status": "TRADED",
                            "attempts": attempt,
                            "correlation_id": filled_correlations[-1] if filled_correlations else attempt_correlation_id,
                            "correlation_ids": filled_correlations[:],
                            "average_price": aggregate_price,
                            "traded_quantity": total_traded,
                            "requested_quantity": requested_total,
                            "remaining_quantity": 0,
                        }
                    if refreshed_status == "TRADED_UNCONFIRMED":
                        return last_result
                    self.event("WARN", f"Cancel before retry failed for {instrument.symbol}: {exc}")
            await asyncio.sleep(0.25 * attempt)
        if total_traded > 0:
            aggregate_price = round(total_value / total_traded, 4) if total_value > 0 else float(reference_price or 0)
            return {
                **last_result,
                "order_id": order_ids[-1] if order_ids else str(last_result.get("order_id") or ""),
                "order_ids": order_ids[:],
                "status": "PARTIALLY_FILLED",
                "attempts": 3,
                "correlation_id": filled_correlations[-1] if filled_correlations else str(last_result.get("correlation_id") or ""),
                "correlation_ids": filled_correlations[:],
                "average_price": aggregate_price,
                "traded_quantity": total_traded,
                "requested_quantity": requested_total,
                "remaining_quantity": max(0, requested_total - total_traded),
            }
        reason = self._order_failure_reason(last_result)
        suffix = f": {reason}" if reason else ""
        self.event("ERROR", f"{instrument.symbol} {transaction_type} failed after 3 attempts: {last_result.get('status')}{suffix}")
        return last_result

    async def _refresh_after_cancel_reject(
        self,
        instrument: Instrument,
        order_id: str,
        correlation_id: str,
        fallback_status: str,
        reference_price: float,
        cancel_error: Exception,
    ) -> dict[str, Any]:
        error_message = str(cancel_error)[:800]
        implied_status = self._cancel_reject_implied_status(cancel_error)
        detail = await self._order_detail(order_id)
        detail_status = _broker_order_status(detail, implied_status or fallback_status)
        status = implied_status or detail_status or fallback_status or "UNKNOWN"
        fill_details = await self._order_fill_details(
            order_id,
            reference_price if status in FILL_ACCEPTED_STATUSES else 0,
        )
        traded_quantity = int(fill_details.get("traded_quantity") or 0)
        requested_quantity = int(((self.ledger.get("orders") or {}).get(correlation_id) or {}).get("quantity") or 0)
        if traded_quantity > 0 and requested_quantity > 0 and traded_quantity < requested_quantity:
            status = "PARTIALLY_FILLED"
        if implied_status in TRADED_STATUSES and traded_quantity <= 0:
            status = "TRADED_UNCONFIRMED"
            error_message = (
                f"{error_message} | Dhan says order traded, but traded quantity could not be confirmed. "
                "Retry stopped to avoid duplicate quantity; broker reconcile must repair state."
            )[:1000]
            self.event("ERROR", f"{instrument.symbol} order {order_id} traded at broker but quantity is unconfirmed; retry stopped.")
        elif status in FILL_ACCEPTED_STATUSES and traded_quantity > 0:
            self.event("INFO", f"{instrument.symbol} order {order_id} traded while cancel was being requested; using broker fill and stopping retry.")
        elif status in FAILED_STATUSES:
            error_message = f"{error_message} | Broker status after cancel reject: {status}"[:1000]
        result = {
            "order_id": order_id,
            "status": status,
            "raw_detail": detail,
            "cancel_error": error_message,
            "error_message": error_message,
            "average_price": fill_details.get("average_price") or 0.0,
            "traded_quantity": traded_quantity,
        }
        self._update_ledger_order(
            correlation_id,
            {
                "status": status,
                "raw_detail": detail,
                "cancel_error": error_message,
                "error_message": error_message,
                "average_price": result["average_price"] or None,
                "traded_quantity": traded_quantity or None,
            },
        )
        return result

    def _cancel_reject_implied_status(self, exc: Exception) -> str:
        text = str(exc).lower()
        if "traded" in text:
            return "TRADED"
        if "cancelled" in text or "canceled" in text:
            return "CANCELLED"
        return ""

    async def _order_detail(self, order_id: str) -> dict[str, Any]:
        if not order_id or order_id.startswith("DRY-"):
            return {}
        try:
            assert self.client is not None
            data = await self.client.order_by_id(order_id)
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            return {"error": str(exc)[:500]}

    async def _order_fill_details(self, order_id: str, fallback_price: float = 0.0) -> dict[str, Any]:
        if not order_id or order_id.startswith("DRY-"):
            return {"average_price": float(fallback_price or 0), "traded_quantity": 0}
        average_price = 0.0
        traded_quantity = 0
        try:
            assert self.client is not None
            trades = await self.client.trades_by_order(order_id)
            total_value = 0.0
            total_qty = 0
            for trade in trades:
                qty = _broker_quantity(trade, "tradedQuantity", "tradedQty", "quantity", "qty")
                price = _broker_price(trade, "tradedPrice", "price", "tradePrice")
                if qty > 0 and price > 0:
                    total_qty += qty
                    total_value += qty * price
            if total_qty > 0:
                traded_quantity = total_qty
                average_price = total_value / total_qty
        except Exception:
            pass
        if average_price <= 0:
            try:
                assert self.client is not None
                row = await self.client.order_by_id(order_id)
                average_price = self._average_price_from_order(row)
                traded_quantity = _broker_filled_quantity(row)
            except Exception:
                pass
        if average_price <= 0:
            average_price = float(fallback_price or 0)
        return {"average_price": round(average_price, 4), "traded_quantity": traded_quantity}

    async def _wait_order_status(self, order_id: str, initial_status: str) -> str:
        if not order_id:
            return initial_status or "UNKNOWN"
        deadline = time.monotonic() + 2.5
        status = initial_status or "UNKNOWN"
        while time.monotonic() < deadline:
            update = self.order_updates.get(order_id)
            if update:
                status = str(update.get("Status") or update.get("OrderStatus") or update.get("orderStatus") or status).upper()
                if status in FILL_ACCEPTED_STATUSES | FAILED_STATUSES:
                    return status
            await asyncio.sleep(0.1)
        try:
            assert self.client is not None
            data = await self.client.order_by_id(order_id)
            status = str(data.get("orderStatus") or (data.get("data") or {}).get("orderStatus") or status).upper()
        except Exception:
            pass
        return status

    async def _order_update_worker(self) -> None:
        while self.running and self.credentials.get("client_id") and self.credentials.get("access_token"):
            connected_at = 0.0
            self.order_connecting = True
            try:
                connect_kwargs = {
                    "ping_interval": ORDER_SOCKET_PING_INTERVAL_SECONDS,
                    "ping_timeout": ORDER_SOCKET_PING_TIMEOUT_SECONDS,
                    "open_timeout": ORDER_SOCKET_OPEN_TIMEOUT_SECONDS,
                    "close_timeout": 5,
                    "compression": None,
                    "max_queue": None,
                    "user_agent_header": "KojuDhanAlgo/1.0",
                }
                if "proxy" in inspect.signature(websockets.connect).parameters:
                    connect_kwargs["proxy"] = None
                async with websockets.connect(DHAN_ORDER_UPDATE_URL, **connect_kwargs) as ws:
                    connected_at = time.monotonic()
                    await ws.send(json.dumps({"LoginReq": {"MsgCode": 42, "ClientId": self.credentials["client_id"], "Token": self.credentials["access_token"]}, "UserType": "SELF"}))
                    self.order_connected = True
                    self.order_connecting = False
                    self.order_last_error = ""
                    self.event("INFO", "Dhan order update WebSocket connected.")
                    async for message in ws:
                        if self.order_reconnects and time.monotonic() - connected_at >= 30:
                            self.order_reconnects = 0
                        for payload in _json_objects_from_message(message):
                            if not isinstance(payload, dict):
                                continue
                            if payload.get("errorCode") == "DH-901" or payload.get("errorType") == "Invalid_Authentication":
                                self._handle_auth_failure("Dhan order WebSocket", DhanAuthenticationError(json.dumps(payload)[:300]))
                                return
                            data = payload.get("Data") or payload.get("data") or payload
                            if not isinstance(data, dict):
                                continue
                            order_no = _broker_order_id(data)
                            if order_no:
                                self.order_updates[order_no] = data
                                self._update_ledger_order_from_broker(data)
                    raise RuntimeError("order update socket closed by server")
            except asyncio.CancelledError:
                break
            except DhanAuthenticationError as exc:
                self.order_connecting = False
                self._handle_auth_failure("Dhan order WebSocket", exc)
                break
            except Exception as exc:
                self.order_connecting = False
                if _is_auth_error(exc):
                    self._handle_auth_failure("Dhan order WebSocket", exc)
                    break
                self.order_connected = False
                if connected_at and time.monotonic() - connected_at >= 30:
                    self.order_reconnects = 0
                self.order_reconnects += 1
                self.order_last_error = f"Order update socket reconnecting: {exc}. REST order book/trade book fallback remains active."
                if self.order_reconnects == 1 or self.order_reconnects % 10 == 0:
                    self.event("INFO", self.order_last_error)
                await asyncio.sleep(min(ORDER_SOCKET_RECONNECT_MAX_SECONDS, ORDER_SOCKET_RECONNECT_BASE_SECONDS + self.order_reconnects))
        self.order_connecting = False

    async def _broker_reconcile_worker(self) -> None:
        while self.running:
            try:
                await asyncio.wait_for(
                    self.reconcile_broker_state(),
                    timeout=BROKER_RECONCILE_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self.entries_blocked_until_reconcile = False
                self.broker_reconcile_status = {
                    **self.broker_reconcile_status,
                    "running": False,
                    "message": f"Broker reconcile timed out after {BROKER_RECONCILE_TIMEOUT_SECONDS:g}s; trading continues with existing symbol locks.",
                    "last_run": datetime.now(IST).isoformat(),
                    "entries_blocked_until_reconcile": False,
                }
                self.event("WARN", self.broker_reconcile_status["message"])
            except DhanAuthenticationError as exc:
                self._handle_auth_failure("Broker reconciliation", exc)
                break
            except Exception as exc:
                if _is_auth_error(exc):
                    self._handle_auth_failure("Broker reconciliation", exc)
                    break
                self.broker_reconcile_status = {
                    **self.broker_reconcile_status,
                    "running": False,
                    "message": f"Broker reconcile failed: {exc}; trading continues with existing symbol locks.",
                    "last_run": datetime.now(IST).isoformat(),
                    "entries_blocked_until_reconcile": False,
                }
                self.entries_blocked_until_reconcile = False
                self.event("WARN", self.broker_reconcile_status["message"])
            await asyncio.sleep(20)

    async def reconcile_broker_state(self, startup: bool = False) -> dict[str, Any]:
        if self.settings.dry_run:
            self.locked_symbols = set()
            self.entries_blocked_until_reconcile = False
            self.broker_reconcile_status = {
                "running": False,
                "message": "Dry run: broker reconciliation skipped",
                "last_run": datetime.now(IST).isoformat(),
                "mismatches": [],
                "pending_orders": [],
                "failed_orders": [],
                "locked_symbols": [],
                "entries_blocked_until_reconcile": False,
            }
            return self.broker_reconcile_status
        if not self.client:
            raise RuntimeError("Dhan client is not configured.")
        timings: dict[str, float] = {}
        self.broker_reconcile_status = {
            **self.broker_reconcile_status,
            "running": True,
            "message": "Checking broker positions",
        }
        broker_positions = await self._timed_broker_call(
            "positions",
            self.client.positions,
            timings,
            timeout=BROKER_POSITIONS_TIMEOUT_SECONDS,
        )
        broker_position_details = self._broker_position_details(broker_positions)
        broker_net = {security_id: int(row.get("quantity") or 0) for security_id, row in broker_position_details.items()}
        active_ledger_orders = self._active_ledger_orders()
        trade_book: list[dict[str, Any]] = []
        trade_book_available = True
        trade_book_error = ""
        trade_recovery = {"orders_updated": 0, "unmatched_trades": 0, "partial_orders": 0}
        self.broker_reconcile_status = {
            **self.broker_reconcile_status,
            "message": "Checking broker trade book",
            "timings": timings,
        }
        try:
            trade_book = await self._timed_broker_call(
                "trade book",
                self.client.trade_book,
                timings,
                timeout=BROKER_TRADE_BOOK_TIMEOUT_SECONDS,
            )
            trade_recovery = self._apply_trade_book_fills(trade_book)
        except Exception as exc:
            trade_book_available = False
            trade_book_error = (
                f"trade book timed out after {BROKER_TRADE_BOOK_TIMEOUT_SECONDS:g}s"
                if isinstance(exc, asyncio.TimeoutError)
                else str(exc)
            )
            self.event("WARN", f"Broker trade book unavailable: {trade_book_error}")
        order_book: list[dict[str, Any]] = []
        order_book_available = True
        order_book_error = ""
        pending_recovery = {"matched": 0, "external_checked": 0, "stale_marked": 0, "deferred": 0, "skipped": 0}
        self.broker_reconcile_status = {
            **self.broker_reconcile_status,
            "message": "Checking broker order book",
            "timings": timings,
        }
        try:
            order_book = await self._timed_broker_call(
                "order book",
                self.client.order_book,
                timings,
                timeout=BROKER_ORDER_BOOK_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            order_book_available = False
            order_book_error = (
                f"order book timed out after {BROKER_ORDER_BOOK_TIMEOUT_SECONDS:g}s"
                if isinstance(exc, asyncio.TimeoutError)
                else str(exc)
            )
            self.event("WARN", f"Broker order book unavailable: {order_book_error}")
            if not ALLOW_POSITIONS_ONLY_RECONCILE:
                raise RuntimeError(f"Dhan order book unavailable and broker reconciliation cannot safely continue: {order_book_error}") from exc
            pending_recovery = {"matched": 0, "external_checked": 0, "stale_marked": 0, "deferred": len(active_ledger_orders), "skipped": 1}
        else:
            for row in order_book:
                self._update_ledger_order_from_broker(row)
            self.broker_reconcile_status = {
                **self.broker_reconcile_status,
                "message": "Recovering pending ledger orders",
                "timings": timings,
            }
            try:
                pending_recovery = await asyncio.wait_for(
                    self._recover_pending_ledger_orders(order_book, timings),
                    timeout=BROKER_PENDING_RECOVERY_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                pending_recovery = {"matched": 0, "external_checked": 0, "stale_marked": 0, "deferred": len(active_ledger_orders), "skipped": 0}
                order_book_error = f"Pending ledger recovery timed out after {BROKER_PENDING_RECOVERY_TIMEOUT_SECONDS:g}s"
                self.event("WARN", order_book_error)
        self._apply_unapplied_traded_orders()
        app_net = self._app_net_quantities()
        app_avg = self._app_average_prices()
        relevant_sids = set(app_net) | set(broker_net) | self._ledger_security_ids() | set(self.instruments_by_security)
        mismatches = []
        for security_id in sorted(relevant_sids):
            app_qty = int(app_net.get(security_id, 0))
            broker_qty = int(broker_net.get(security_id, 0))
            if app_qty != broker_qty:
                mismatches.append(
                    {
                        "symbol": self._symbol_for_security(security_id),
                        "security_id": security_id,
                        "app_qty": app_qty,
                        "broker_qty": broker_qty,
                    }
                )
        active_after_recovery = self._active_ledger_orders()
        if order_book_available:
            pending_orders, failed_orders = self._relevant_order_lists(order_book)
            if active_after_recovery:
                pending_orders = self._merge_order_items(pending_orders, self._pending_items_from_ledger(active_after_recovery))
        else:
            pending_orders = self._pending_items_from_ledger(active_after_recovery)
            failed_orders = []
        self.entries_blocked_until_reconcile = False
        ledger_security_ids = self._ledger_security_ids()
        external_positions = self._external_broker_positions(broker_net, app_net, ledger_security_ids)
        avg_syncs = self._sync_average_price_mismatches(broker_position_details, app_avg, app_net)
        new_locked = {
            row["symbol"]
            for row in mismatches
            if row.get("symbol")
        } | {
            row.get("symbol", "")
            for row in pending_orders
            if row.get("symbol")
        } | {
            row.get("symbol", "")
            for row in external_positions
            if row.get("symbol")
        }
        previous_locked = set(self.locked_symbols)
        self.locked_symbols = {symbol for symbol in new_locked if symbol}
        if self.locked_symbols and self.locked_symbols != previous_locked:
            self.event("ERROR", f"Broker/app mismatch lock active: {', '.join(sorted(self.locked_symbols))}")
        if mismatches or pending_orders or external_positions:
            message = (
                f"Broker reconcile locked {len(self.locked_symbols)} symbols | "
                f"mismatches {len(mismatches)} | pending {len(pending_orders)} | "
                f"external {len(external_positions)} | avg synced {len(avg_syncs)}"
            )
        elif avg_syncs:
            message = f"Broker reconcile OK | avg synced {len(avg_syncs)}"
        elif order_book_available:
            message = "Broker reconcile OK"
        else:
            message = f"Broker reconcile positions-only OK; Dhan order book unavailable: {order_book_error}"
        if startup and (mismatches or pending_orders):
            message = "Startup " + message.lower()
        self.broker_positions = broker_net
        self.broker_reconcile_status = {
            "running": False,
            "message": message,
            "last_run": datetime.now(IST).isoformat(),
            "mismatches": mismatches,
            "pending_orders": pending_orders,
            "failed_orders": failed_orders,
            "external_positions": external_positions,
            "avg_mismatches": [],
            "avg_syncs": avg_syncs,
            "locked_symbols": sorted(self.locked_symbols),
            "entries_blocked_until_reconcile": self.entries_blocked_until_reconcile,
            "timings": timings,
            "pending_recovery": pending_recovery,
            "trade_recovery": trade_recovery,
            "trade_book_available": trade_book_available,
            "trade_book_error": trade_book_error,
            "order_book_available": order_book_available,
            "order_book_error": order_book_error,
        }
        return self.broker_reconcile_status

    async def _timed_broker_call(self, label: str, factory: Any, timings: dict[str, float], timeout: float | None = None) -> Any:
        started = time.monotonic()
        result = await asyncio.wait_for(factory(), timeout=timeout) if timeout else await factory()
        elapsed = round(time.monotonic() - started, 3)
        timings[label] = elapsed
        if elapsed >= 5:
            self.event("WARN", f"Broker reconcile {label} took {elapsed:.1f}s.")
        return result

    def _pending_items_from_ledger(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items = []
        for record in records:
            security_id = str(record.get("security_id") or "")
            items.append(
                {
                    "symbol": str(record.get("symbol") or self._symbol_for_security(security_id)),
                    "security_id": security_id,
                    "order_id": str(record.get("order_id") or ""),
                    "correlation_id": str(record.get("correlation_id") or ""),
                    "status": str(record.get("status") or "PENDING_ORDER").upper(),
                    "transaction_type": _broker_transaction_type({"transactionType": record.get("transaction_type")}),
                    "quantity": int(record.get("quantity") or 0),
                }
            )
        return items

    def _merge_order_items(self, first: list[dict[str, Any]], second: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in [*first, *second]:
            key = (
                str(item.get("correlation_id") or ""),
                str(item.get("order_id") or ""),
                str(item.get("security_id") or ""),
            )
            merged[key] = item
        return list(merged.values())

    def _apply_trade_book_fills(self, trades: list[dict[str, Any]]) -> dict[str, int]:
        fills: dict[str, dict[str, Any]] = {}
        for trade in trades:
            order_id = _broker_order_id(trade)
            if not order_id:
                continue
            qty = _broker_quantity(trade, "tradedQuantity", "tradedQty", "filledQty", "quantity", "qty")
            price = _broker_price(trade, "tradedPrice", "tradePrice", "price", "averageTradedPrice", "averagePrice")
            if qty <= 0:
                continue
            item = fills.setdefault(
                order_id,
                {
                    "order_id": order_id,
                    "quantity": 0,
                    "value": 0.0,
                    "raw_trades": [],
                },
            )
            item["quantity"] += qty
            if price > 0:
                item["value"] += qty * price
            item["raw_trades"].append(trade)
        orders_updated = 0
        unmatched = 0
        partial = 0
        for order_id, fill in fills.items():
            correlation_id, record = self._ledger_record_by_order_id(order_id)
            if not correlation_id or not record:
                unmatched += 1
                continue
            traded_quantity = int(fill["quantity"])
            requested_quantity = int(record.get("quantity") or 0)
            average_price = (float(fill["value"]) / traded_quantity) if traded_quantity > 0 and fill["value"] else 0.0
            status = "TRADED"
            if requested_quantity > 0 and traded_quantity < requested_quantity:
                status = "PARTIALLY_FILLED"
                partial += 1
            self._update_ledger_order(
                correlation_id,
                {
                    "order_id": order_id,
                    "status": status,
                    "average_price": round(average_price, 4) if average_price > 0 else None,
                    "traded_quantity": traded_quantity,
                    "remaining_quantity": max(0, requested_quantity - traded_quantity) if requested_quantity else None,
                    "raw_trades": fill["raw_trades"],
                },
            )
            orders_updated += 1
        return {"orders_updated": orders_updated, "unmatched_trades": unmatched, "partial_orders": partial}

    def _ledger_record_by_order_id(self, order_id: str) -> tuple[str, dict[str, Any] | None]:
        for correlation_id, record in (self.ledger.get("orders") or {}).items():
            if order_id and str(record.get("order_id") or "") == order_id:
                return correlation_id, record
        return "", None

    def _ledger_order_is_from_today(self, record: dict[str, Any]) -> bool:
        today = _today_key()
        for field in ("created_at", "updated_at"):
            value = str(record.get(field) or "")
            if value.startswith(today):
                return True
        return False

    def _active_ledger_orders(self) -> list[dict[str, Any]]:
        active = []
        for correlation_id, record in list((self.ledger.get("orders") or {}).items()):
            status = str(record.get("status") or "").upper()
            if status in FINAL_ORDER_STATUSES:
                continue
            if not self._ledger_order_is_from_today(record):
                self._update_ledger_order(
                    correlation_id,
                    {
                        "status": "STALE_UNRESOLVED",
                        "error_message": "Pending order was not present in current Dhan order book and is from a previous session.",
                    },
                )
                continue
            active.append(record)
        return active

    async def _recover_pending_ledger_orders(self, order_book: list[dict[str, Any]], timings: dict[str, float] | None = None) -> dict[str, int]:
        by_correlation = {_broker_correlation_id(row): row for row in order_book if _broker_correlation_id(row)}
        by_order_id = {_broker_order_id(row): row for row in order_book if _broker_order_id(row)}
        checked_external = 0
        matched = 0
        stale = 0
        deferred = 0
        for correlation_id, record in list((self.ledger.get("orders") or {}).items()):
            status = str(record.get("status") or "").upper()
            if status in FINAL_ORDER_STATUSES:
                continue
            row = by_correlation.get(correlation_id) or by_order_id.get(str(record.get("order_id") or ""))
            if not row and not self._ledger_order_is_from_today(record):
                self._update_ledger_order(
                    correlation_id,
                    {
                        "status": "STALE_UNRESOLVED",
                        "error_message": "Pending order was not present in current Dhan order book and is from a previous session.",
                    },
                )
                stale += 1
                continue
            if not row and correlation_id:
                if checked_external >= BROKER_PENDING_LOOKUP_LIMIT:
                    deferred += 1
                    continue
                try:
                    assert self.client is not None
                    started = time.monotonic()
                    row = await self.client.order_by_correlation_id(correlation_id)
                    checked_external += 1
                    if timings is not None:
                        timings["pending external lookups"] = round(timings.get("pending external lookups", 0) + (time.monotonic() - started), 3)
                except Exception:
                    checked_external += 1
                    row = None
            if isinstance(row, dict) and row:
                self._update_ledger_order_from_broker(row, correlation_id)
                matched += 1
        return {"matched": matched, "external_checked": checked_external, "stale_marked": stale, "deferred": deferred}

    def _apply_unapplied_traded_orders(self) -> None:
        for correlation_id, record in list((self.ledger.get("orders") or {}).items()):
            status = str(record.get("status") or "").upper()
            if status not in FILL_ACCEPTED_STATUSES:
                continue
            role = str(record.get("order_role") or "").upper()
            position_key = str(record.get("position_key") or "")
            metadata = record.get("metadata") or {}
            fill_price = float(record.get("average_price") or metadata.get("reference_price") or 0)
            traded_quantity = int(record.get("traded_quantity") or 0)
            if traded_quantity <= 0 and status in TRADED_STATUSES:
                traded_quantity = int(record.get("quantity") or 0)
            applied_quantity = int(record.get("applied_quantity") or 0)
            quantity = max(0, traded_quantity - applied_quantity)
            if quantity <= 0:
                if status in TRADED_STATUSES and not record.get("applied_to_position"):
                    self._mark_latest_order_applied(correlation_id)
                continue
            if role == "ENTRY":
                self._recover_entry_position(position_key, record, metadata, fill_price, quantity)
            elif role == "PYRAMID":
                self._recover_pyramid_position(position_key, record, fill_price, quantity)
            elif role == "EXIT":
                position = self.positions.get(position_key)
                if position:
                    position.exit_order_id = str(record.get("order_id") or "")
                    if quantity < position.quantity:
                        position.quantity -= quantity
                        position.exit_reason = f"PARTIAL_{str(metadata.get('reason') or 'RECOVERED_EXIT')}"
                    else:
                        position.status = "CLOSED"
                        position.exit_reason = str(metadata.get("reason") or "RECOVERED_EXIT")
                    self._persist_position(position)
            self._mark_latest_order_applied(correlation_id)

    def _recover_entry_position(self, position_key: str, record: dict[str, Any], metadata: dict[str, Any], fill_price: float, quantity: int) -> None:
        if position_key in self.positions and self.positions[position_key].status == "OPEN":
            return
        payload = metadata.get("position") or {}
        side = str(payload.get("side") or ("SHORT" if _broker_transaction_type(record) == "SELL" else "LONG")).upper()
        symbol = str(record.get("symbol") or payload.get("symbol") or "")
        entry = float(fill_price or payload.get("entry_price") or 0)
        stop = float(payload.get("stop_loss") or 0)
        target = float(payload.get("target") or 0)
        position = Position(
            symbol=symbol,
            security_id=str(record.get("security_id") or ""),
            side=side,
            strategy=str(payload.get("strategy") or "recovered_entry"),
            entry_price=round(entry, 2),
            quantity=max(0, int(quantity or payload.get("quantity") or 0)),
            stop_loss=round(stop, 2),
            target=round(target, 2),
            opened_at=str(record.get("updated_at") or datetime.now(IST).isoformat()),
            order_id=str(record.get("order_id") or ""),
            last_price=round(entry, 2),
            meta={"recovered": True, **{k: v for k, v in payload.items() if k not in {"symbol"}}},
        )
        if position.symbol and position.quantity > 0:
            self.positions[position_key or self._position_key(position.symbol, position.side)] = position
            self._persist_position(position)

    def _recover_pyramid_position(self, position_key: str, record: dict[str, Any], fill_price: float, quantity: int) -> None:
        position = self.positions.get(position_key)
        if not position or quantity <= 0:
            return
        old_quantity = int(position.quantity)
        new_quantity = old_quantity + quantity
        price = float(fill_price or position.last_price or position.entry_price)
        if new_quantity > 0:
            position.entry_price = round(((position.entry_price * old_quantity) + (price * quantity)) / new_quantity, 2)
            position.quantity = new_quantity
            position.meta["pyramid_adds"] = int(position.meta.get("pyramid_adds") or 0) + 1
            position.meta.setdefault("pyramid_orders", []).append(
                {"order_id": str(record.get("order_id") or ""), "quantity": quantity, "price": price, "status": record.get("status")}
            )
            if position.strategy in {"scalper_long", "scalper_short"}:
                self._refresh_scalper_risk(position)
            self._persist_position(position)

    def _broker_net_quantities(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        broker_net: dict[str, int] = {}
        for row in rows:
            security_id = _broker_security_id(row)
            if not security_id:
                continue
            product_type = _broker_product_type(row)
            if product_type and product_type != "INTRADAY":
                continue
            net_qty = _broker_quantity(row, "netQty", "netQuantity")
            if net_qty:
                broker_net[security_id] = broker_net.get(security_id, 0) + net_qty
        return broker_net

    def _broker_position_details(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        details: dict[str, dict[str, Any]] = {}
        for row in rows:
            security_id = _broker_security_id(row)
            if not security_id:
                continue
            product_type = _broker_product_type(row)
            if product_type and product_type != "INTRADAY":
                continue
            net_qty = _broker_quantity(row, "netQty", "netQuantity")
            if not net_qty:
                continue
            avg_price = self._broker_position_average(row, net_qty)
            item = details.setdefault(
                security_id,
                {
                    "security_id": security_id,
                    "symbol": self._symbol_for_security(security_id),
                    "quantity": 0,
                    "average_price": 0.0,
                    "raw": [],
                },
            )
            old_qty = abs(int(item["quantity"]))
            new_qty = abs(int(net_qty))
            if avg_price > 0:
                total_qty = old_qty + new_qty
                item["average_price"] = ((float(item["average_price"]) * old_qty) + (avg_price * new_qty)) / total_qty if total_qty else avg_price
            item["quantity"] = int(item["quantity"]) + net_qty
            item["raw"].append(row)
        for item in details.values():
            item["average_price"] = round(float(item.get("average_price") or 0), 4)
        return details

    def _broker_position_average(self, row: dict[str, Any], net_qty: int) -> float:
        if net_qty < 0:
            return _broker_price(row, "sellAvg", "sellAvgPrice", "sellAveragePrice", "averagePrice", "avgPrice", "netAvg", "netAveragePrice")
        return _broker_price(row, "buyAvg", "buyAvgPrice", "buyAveragePrice", "averagePrice", "avgPrice", "netAvg", "netAveragePrice")

    def _app_net_quantities(self) -> dict[str, int]:
        app_net: dict[str, int] = {}
        for position in self.positions.values():
            if position.status != "OPEN":
                continue
            sign = -1 if position.side == "SHORT" else 1
            app_net[str(position.security_id)] = app_net.get(str(position.security_id), 0) + sign * int(position.quantity)
        return app_net

    def _app_average_prices(self) -> dict[str, float]:
        totals: dict[str, dict[str, float]] = {}
        for position in self.positions.values():
            if position.status != "OPEN":
                continue
            security_id = str(position.security_id)
            qty = abs(int(position.quantity or 0))
            price = float(position.entry_price or 0)
            if qty <= 0 or price <= 0:
                continue
            item = totals.setdefault(security_id, {"quantity": 0.0, "value": 0.0})
            item["quantity"] += qty
            item["value"] += qty * price
        return {
            security_id: round(row["value"] / row["quantity"], 4)
            for security_id, row in totals.items()
            if row["quantity"] > 0
        }

    def _external_broker_positions(self, broker_net: dict[str, int], app_net: dict[str, int], ledger_security_ids: set[str]) -> list[dict[str, Any]]:
        external = []
        for security_id, broker_qty in sorted(broker_net.items()):
            if int(broker_qty or 0) == 0:
                continue
            if int(app_net.get(security_id, 0)) != 0:
                continue
            if security_id in ledger_security_ids:
                continue
            external.append(
                {
                    "symbol": self._symbol_for_security(security_id),
                    "security_id": security_id,
                    "broker_qty": int(broker_qty),
                    "reason": "Broker intraday position has no app ledger/order correlation.",
                }
            )
        return external

    def _sync_average_price_mismatches(
        self,
        broker_details: dict[str, dict[str, Any]],
        app_avg: dict[str, float],
        app_net: dict[str, int],
    ) -> list[dict[str, Any]]:
        synced = []
        for security_id, broker in sorted(broker_details.items()):
            broker_qty = int(broker.get("quantity") or 0)
            app_qty = int(app_net.get(security_id, 0))
            if broker_qty == 0 or broker_qty != app_qty:
                continue
            broker_avg = float(broker.get("average_price") or 0)
            local_avg = float(app_avg.get(security_id) or 0)
            if broker_avg <= 0 or local_avg <= 0:
                continue
            diff = abs(broker_avg - local_avg)
            if diff >= max(0.05, broker_avg * 0.001):
                sync_rows = self._sync_open_position_average(security_id, broker_avg)
                synced.append(
                    {
                        "symbol": self._symbol_for_security(security_id),
                        "security_id": security_id,
                        "broker_qty": broker_qty,
                        "broker_avg": round(broker_avg, 4),
                        "app_avg": round(local_avg, 4),
                        "diff": round(diff, 4),
                        "positions_synced": sync_rows,
                    }
                )
        return synced

    def _sync_open_position_average(self, security_id: str, broker_avg: float) -> int:
        updated = 0
        for position in self.positions.values():
            if position.status != "OPEN" or str(position.security_id) != str(security_id):
                continue
            old_entry = float(position.entry_price or 0)
            position.entry_price = round(float(broker_avg), 2)
            position.last_price = position.last_price or position.entry_price
            position.meta["broker_avg_synced"] = True
            position.meta["previous_app_entry_price"] = old_entry
            position.meta["broker_average_price"] = round(float(broker_avg), 4)
            self._persist_position(position)
            updated += 1
        return updated

    def _ledger_security_ids(self) -> set[str]:
        ids = {str(record.get("security_id") or "") for record in (self.ledger.get("orders") or {}).values()}
        ids.update(str(row.get("security_id") or "") for row in (self.ledger.get("positions") or {}).values())
        return {item for item in ids if item}

    def _symbol_for_security(self, security_id: str) -> str:
        instrument = self.instruments_by_security.get(str(security_id))
        if instrument:
            return instrument.symbol
        for position in self.positions.values():
            if str(position.security_id) == str(security_id):
                return position.symbol
        for record in (self.ledger.get("orders") or {}).values():
            if str(record.get("security_id") or "") == str(security_id):
                return str(record.get("symbol") or security_id)
        return str(security_id)

    def _relevant_order_lists(self, order_book: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        ledger_correlations = set((self.ledger.get("orders") or {}).keys())
        relevant_security_ids = self._ledger_security_ids() | set(self.instruments_by_security)
        pending = []
        failed = []
        for row in order_book:
            correlation_id = _broker_correlation_id(row)
            security_id = _broker_security_id(row)
            if correlation_id not in ledger_correlations and security_id not in relevant_security_ids:
                continue
            status = _broker_order_status(row)
            item = {
                "symbol": self._symbol_for_security(security_id),
                "security_id": security_id,
                "order_id": _broker_order_id(row),
                "correlation_id": correlation_id,
                "status": status,
                "transaction_type": _broker_transaction_type(row),
                "quantity": _broker_quantity(row, "quantity"),
            }
            if status in PENDING_STATUSES or status in PENDING_LEDGER_STATUSES:
                pending.append(item)
            elif status in FAILED_STATUSES:
                failed.append(item)
        return pending, failed

    async def _auto_reconcile_worker(self) -> None:
        while self.running:
            try:
                summary = await self.reconcile_all_missing_candles()
                self.reconcile_status = {
                    "running": False,
                    "message": "Auto reconcile complete",
                    "last_run": datetime.now(IST).isoformat(),
                    "last_missing": summary,
                }
            except asyncio.CancelledError:
                break
            except DhanAuthenticationError as exc:
                self._handle_auth_failure("Auto reconcile", exc)
                break
            except Exception as exc:
                if _is_auth_error(exc):
                    self._handle_auth_failure("Auto reconcile", exc)
                    break
                self.reconcile_status = {
                    "running": False,
                    "message": f"Auto reconcile failed: {exc}",
                    "last_run": datetime.now(IST).isoformat(),
                    "last_missing": {},
                }
                self.event("WARN", self.reconcile_status["message"])
            await asyncio.sleep(60)

    async def run_premarket_cache(self, force: bool = False) -> dict[str, Any]:
        if not self.client and not (self.credentials.get("client_id") and self.credentials.get("access_token")):
            self.premarket_status = {"running": False, "message": "Credentials missing", "progress": 0}
            return self.premarket_status
        if self.cache_task and not self.cache_task.done() and not force:
            return self.premarket_status
        self._resolve_watchlists()
        universe = sorted(self.universe_symbols or set(self.long_symbols + self.short_symbols))
        if universe:
            resolved, _missing = self.resolver.resolve(universe)
        else:
            resolved = list(self.instruments_by_symbol.values())
        sector_items = list(self.sector_instruments.items())
        cache = read_json(PREMARKET_FILE, {"symbols": {}, "sectors": {}, "errors": {}})
        today_key = datetime.now(IST).date().isoformat()
        required_bars = max(1, self.settings.sma_period)
        summary = _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars)
        stock_work_count = len(resolved) if force else summary["stock_missing"]
        sector_work_count = len(sector_items) if force else summary["sector_missing"]
        if not force and stock_work_count == 0 and sector_work_count == 0:
            report = self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)
            self.premarket_status = {
                "running": False,
                "message": f"Premarket cache already complete | stocks {summary['stock_cached']}/{summary['stock_total']} | sectors {summary['sector_cached']}/{summary['sector_total']}",
                "progress": 100,
                "total": 0,
                "summary": report["summary"],
                "report_file": str(PREMARKET_REPORT_FILE),
            }
            return self.premarket_status
        self.premarket_status = {
            "running": True,
            "message": f"Starting missing cache fetch | stocks {stock_work_count} | sectors {sector_work_count}",
            "progress": 0,
            "total": max(1, stock_work_count + sector_work_count),
            "summary": summary,
            "report_file": str(PREMARKET_REPORT_FILE),
        }
        self.cache_task = asyncio.create_task(self._build_premarket_cache(force=force))
        return self.premarket_status

    async def _build_premarket_cache(self, force: bool = False) -> None:
        if not self.client:
            if self.credentials.get("client_id") and self.credentials.get("access_token"):
                self.client = DhanClient(self.credentials["client_id"], self.credentials["access_token"])
            else:
                self.premarket_status = {"running": False, "message": "Credentials missing", "progress": 0}
                return
        self._resolve_watchlists()
        universe = sorted(self.universe_symbols or set(self.long_symbols + self.short_symbols))
        if universe:
            resolved, _missing = self.resolver.resolve(universe)
        else:
            resolved = list(self.instruments_by_symbol.values())
        sector_items = list(self.sector_instruments.items())
        total = max(1, len(resolved) + len(sector_items))
        cache = read_json(PREMARKET_FILE, {"symbols": {}, "sectors": {}, "errors": {}})
        cache.setdefault("symbols", {})
        cache.setdefault("sectors", {})
        cache.setdefault("errors", {})
        now = datetime.now(IST)
        start = now - timedelta(days=14)
        today_key = now.date().isoformat()
        required_bars = max(1, self.settings.sma_period)
        summary = _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars)
        if force:
            stock_work = resolved
            sector_work = sector_items
        else:
            missing_stock_set = set(summary["stock_missing_all"])
            missing_sector_set = set(summary["sector_missing_all"])
            stock_work = [instrument for instrument in resolved if instrument.symbol in missing_stock_set]
            sector_work = [(name, instrument) for name, instrument in sector_items if name in missing_sector_set]
        work_total = max(1, len(stock_work) + len(sector_work))
        already_cached = (len(resolved) + len(sector_items)) - (len(stock_work) + len(sector_work))
        self.premarket_status = {
            "running": True,
            "message": f"Fetching missing cache | stocks {len(stock_work)} | sectors {len(sector_work)}",
            "progress": 0,
            "total": work_total,
            "summary": summary,
        }
        if not force and not stock_work and not sector_work:
            report = self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)
            self.premarket_status = {
                "running": False,
                "message": f"Premarket cache already complete | stocks {summary['stock_cached']}/{summary['stock_total']} | sectors {summary['sector_cached']}/{summary['sector_total']}",
                "progress": 100,
                "total": 0,
                "summary": report["summary"],
                "report_file": str(PREMARKET_REPORT_FILE),
            }
            return
        completed = 0
        cached = 0
        skipped = already_cached
        failed = 0
        for instrument in stock_work:
            try:
                daily = await self.client.daily(instrument.security_id, start, now, instrument.exchange_segment, instrument.instrument)
                intraday_1 = await self.client.intraday(instrument.security_id, 1, start.replace(hour=9, minute=15, second=0), now)
                intraday_5 = _aggregate_intraday_rows(intraday_1, 5)
                previous = _latest_completed_daily(daily, now)
                bars_1 = intraday_1[-required_bars:]
                bars_5 = intraday_5[-required_bars:]
                cache["symbols"][instrument.symbol] = {
                    "security_id": instrument.security_id,
                    "sector": ", ".join(sectors_for_symbol(instrument.symbol)),
                    "previous_day": {
                        "high": previous.get("high", 0),
                        "low": previous.get("low", 0),
                        "close": previous.get("close", 0),
                    },
                    "latest_cached_date": _row_date(previous),
                    "bars": {
                        "1": bars_1,
                        "5": bars_5,
                    },
                    "baseline": {
                        "1": [int(row.get("volume") or 0) for row in bars_1],
                        "5": [int(row.get("volume") or 0) for row in bars_5],
                    },
                    "updated_at": datetime.now(IST).isoformat(),
                }
                cache["errors"].pop(instrument.symbol, None)
                self.candles.seed_history(instrument, 1, intraday_1)
                self.candles.seed_history(instrument, 5, intraday_5)
                self._mark_reconciled_day_history(instrument, intraday_1, now)
                cached += 1
            except Exception as exc:
                if isinstance(exc, DhanAuthenticationError) or _is_auth_error(exc):
                    self._handle_auth_failure("Premarket cache", exc)
                    self.premarket_status = {
                        "running": False,
                        "message": self.last_error,
                        "progress": round(completed / work_total * 100, 1),
                        "total": work_total,
                        "summary": _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars),
                        "report_file": str(PREMARKET_REPORT_FILE),
                    }
                    write_json(PREMARKET_FILE, cache)
                    self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)
                    return
                failed += 1
                cache["errors"][instrument.symbol] = {
                    "type": exc.__class__.__name__,
                    "message": str(exc)[:500],
                    "updated_at": datetime.now(IST).isoformat(),
                }
                self.event("WARN", f"Premarket cache failed for {instrument.symbol}: {exc}")
            completed += 1
            self.premarket_status = {
                "running": True,
                "message": f"Missing stocks fetched {completed}/{len(stock_work)} | cached {cached} | failed {failed}",
                "progress": round(completed / work_total * 100, 1),
                "total": work_total,
                "summary": _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars),
            }
            write_json(PREMARKET_FILE, cache)
            self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)
        for sector_name, instrument in sector_work:
            try:
                daily = await self.client.daily(instrument.security_id, start, now, instrument.exchange_segment, instrument.instrument)
                previous = _latest_completed_daily(daily, now)
                cache["sectors"][sector_name] = {
                    "name": sector_name,
                    "security_id": instrument.security_id,
                    "segment": instrument.exchange_segment,
                    "instrument": instrument.instrument,
                    "previous_day": {
                        "high": previous.get("high", 0),
                        "low": previous.get("low", 0),
                        "close": previous.get("close", 0),
                    },
                    "latest_cached_date": to_ist(previous.get("timestamp")).date().isoformat() if previous.get("timestamp") else "",
                    "updated_at": datetime.now(IST).isoformat(),
                }
                cache["errors"].pop(sector_name, None)
                cached += 1
            except Exception as exc:
                if isinstance(exc, DhanAuthenticationError) or _is_auth_error(exc):
                    self._handle_auth_failure("Premarket cache", exc)
                    self.premarket_status = {
                        "running": False,
                        "message": self.last_error,
                        "progress": round(completed / work_total * 100, 1),
                        "total": work_total,
                        "summary": _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars),
                        "report_file": str(PREMARKET_REPORT_FILE),
                    }
                    write_json(PREMARKET_FILE, cache)
                    self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)
                    return
                failed += 1
                cache["errors"][sector_name] = {
                    "type": exc.__class__.__name__,
                    "message": str(exc)[:500],
                    "updated_at": datetime.now(IST).isoformat(),
                }
                self.event("WARN", f"Premarket sector cache failed for {sector_name}: {exc}")
            completed += 1
            self.premarket_status = {
                "running": True,
                "message": f"Missing cache fetched {completed}/{work_total} | cached {cached} | failed {failed}",
                "progress": round(completed / work_total * 100, 1),
                "total": work_total,
                "summary": _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars),
            }
            write_json(PREMARKET_FILE, cache)
            self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)
        summary = _premarket_cache_summary(cache, resolved, sector_items, today_key, required_bars)
        complete = summary["stock_missing"] == 0 and summary["sector_missing"] == 0
        message = (
            f"Premarket cache complete | stocks {summary['stock_cached']}/{summary['stock_total']} | sectors {summary['sector_cached']}/{summary['sector_total']}"
            if complete
            else f"Premarket cache incomplete | stocks missing {summary['stock_missing']} | sectors missing {summary['sector_missing']}"
        )
        if not complete:
            missing_bits = []
            if summary["stock_missing_symbols"]:
                missing_bits.append("stocks: " + ", ".join(summary["stock_missing_symbols"][:20]))
            if summary["sector_missing_symbols"]:
                missing_bits.append("sectors: " + ", ".join(summary["sector_missing_symbols"][:20]))
            self.event("WARN", "Premarket cache still missing " + " | ".join(missing_bits))
        self.premarket_status = {
            "running": False,
            "message": message,
            "progress": 100,
            "total": work_total,
            "summary": summary,
            "report_file": str(PREMARKET_REPORT_FILE),
        }
        self._write_premarket_report(cache, resolved, sector_items, today_key, required_bars)

    async def reconcile_missing_candles(self, symbol: str) -> dict[str, Any]:
        if not self.client:
            raise RuntimeError("Dhan client is not configured.")
        instrument = self.instruments_by_symbol.get(normalize_symbol(symbol))
        if not instrument:
            raise RuntimeError("Symbol is not resolved.")
        return await self._reconcile_instrument_missing_candles(instrument)

    async def reconcile_all_missing_candles(self) -> dict[str, dict[str, int]]:
        if not self.client:
            raise RuntimeError("Dhan client is not configured.")
        self._resolve_watchlists()
        self.reconcile_status = {
            "running": True,
            "message": "Checking missing candles",
            "last_run": self.reconcile_status.get("last_run", ""),
            "last_missing": self.reconcile_status.get("last_missing", {}),
        }
        summary: dict[str, dict[str, int]] = {}
        for instrument in list(self.instruments_by_symbol.values()):
            symbol_summary = await self._reconcile_instrument_missing_candles(instrument)
            if any(symbol_summary.values()):
                summary[instrument.symbol] = symbol_summary
        if summary:
            missing_count = sum(sum(values.values()) for values in summary.values())
            self.event("INFO", f"Auto reconciled {missing_count} missing candles across {len(summary)} symbols.")
        return summary

    async def _reconcile_instrument_missing_candles(self, instrument: Instrument, as_of: datetime | None = None) -> dict[str, int]:
        now = as_of or datetime.now(IST)
        completed_until = now.replace(second=0, microsecond=0)
        result: dict[str, int] = {}
        for timeframe in (1, 5):
            existing = {c.start for c in self.candles.closed_candles(instrument.symbol, timeframe)}
            expected = [
                dt for dt in expected_starts_for_day(now, timeframe)
                if dt + timedelta(minutes=timeframe) <= completed_until
            ]
            missing = [dt for dt in expected if dt not in existing]
            if missing:
                fetch_from = self._intraday_reconcile_fetch_from(timeframe, missing, now)
                rows = await self.client.intraday(instrument.security_id, timeframe, fetch_from, now)
                rows = self._market_session_candle_rows(rows, timeframe)
                self.candles.seed_history(instrument, timeframe, rows)
                if timeframe == 1:
                    self._mark_reconciled_day_history(instrument, rows, now)
            result[str(timeframe)] = len(missing)
        self._mark_reconciled_day_history(instrument, [], now)
        return result

    def _intraday_reconcile_fetch_from(self, timeframe: int, missing: list[datetime], now: datetime) -> datetime:
        fetch_from = min(missing)
        if timeframe == 1:
            session_open_dt = now.replace(
                hour=SESSION_OPEN.hour,
                minute=SESSION_OPEN.minute,
                second=0,
                microsecond=0,
            )
            if any(dt == session_open_dt for dt in missing):
                return session_open_dt - timedelta(minutes=1)
        return fetch_from

    def _market_session_candle_rows(self, rows: list[dict[str, Any]], timeframe: int) -> list[dict[str, Any]]:
        filtered = []
        for row in rows:
            raw_timestamp = to_ist(row.get("timestamp"))
            if raw_timestamp.time() < SESSION_OPEN:
                continue
            start = floor_timeframe(raw_timestamp, timeframe)
            if start.time() >= SESSION_OPEN:
                filtered.append(row)
        return filtered

    def _previous_day(self, symbol: str) -> dict[str, float]:
        cache = self._premarket_cache()
        return ((cache.get("symbols") or {}).get(symbol) or {}).get("previous_day") or {}

    def _baseline(self, symbol: str, timeframe: int) -> list[int]:
        cache = self._premarket_cache()
        return (((cache.get("symbols") or {}).get(symbol) or {}).get("baseline") or {}).get(str(timeframe)) or []

    def _premarket_cache(self, max_age_seconds: float = 5.0) -> dict[str, Any]:
        now = time.monotonic()
        if now - self.premarket_cache_checked_at < max_age_seconds:
            return self.premarket_cache_data
        self.premarket_cache_checked_at = now
        try:
            mtime = PREMARKET_FILE.stat().st_mtime
        except OSError:
            self.premarket_cache_data = {"symbols": {}, "sectors": {}, "errors": {}}
            self.premarket_cache_mtime = 0.0
            return self.premarket_cache_data
        if mtime != self.premarket_cache_mtime:
            self.premarket_cache_data = read_json(PREMARKET_FILE, {"symbols": {}, "sectors": {}, "errors": {}})
            self.premarket_cache_mtime = mtime
        return self.premarket_cache_data

    def _premarket_cache_summary_snapshot(self) -> dict[str, Any]:
        universe = sorted(self.universe_symbols or set(self.long_symbols + self.short_symbols))
        if universe:
            resolved, _missing = self.resolver.resolve(universe)
        else:
            resolved = list(self.instruments_by_symbol.values())
        cache = self._premarket_cache()
        today_key = datetime.now(IST).date().isoformat()
        return _premarket_cache_summary(
            cache,
            resolved,
            list(self.sector_instruments.items()),
            today_key,
            max(1, self.settings.sma_period),
        )

    def _write_premarket_report(
        self,
        cache: dict[str, Any],
        stock_instruments: list[Instrument],
        sector_items: list[tuple[str, Instrument]],
        today_key: str,
        required_bars: int,
    ) -> dict[str, Any]:
        summary = _premarket_cache_summary(cache, stock_instruments, sector_items, today_key, required_bars)
        report = {
            "generated_at": datetime.now(IST).isoformat(),
            "cache_file": str(PREMARKET_FILE),
            "summary": summary,
            "missing": {
                "stocks": summary["stock_missing_all"],
                "sectors": summary["sector_missing_all"],
            },
            "failed": {
                "stocks": summary["failed_symbols_all"],
                "sectors": summary["failed_sectors_all"],
            },
        }
        write_json(PREMARKET_REPORT_FILE, report)
        return report

    def premarket_report(self) -> dict[str, Any]:
        cache = read_json(PREMARKET_FILE, {"symbols": {}, "sectors": {}, "errors": {}})
        today_key = datetime.now(IST).date().isoformat()
        universe = sorted(self.universe_symbols or set(self.long_symbols + self.short_symbols))
        if universe:
            resolved, _missing = self.resolver.resolve(universe)
        else:
            resolved = list(self.instruments_by_symbol.values())
        return self._write_premarket_report(
            cache,
            resolved,
            list(self.sector_instruments.items()),
            today_key,
            max(1, self.settings.sma_period),
        )

    def _previous_sector_day(self, sector: str) -> dict[str, float]:
        cache = self._premarket_cache()
        return ((cache.get("sectors") or {}).get(sector) or {}).get("previous_day") or {}

    def _on_sector_tick(self, security_id: str, price: float, timestamp: datetime) -> None:
        sector_name = self.sector_security_to_name.get(str(security_id))
        if not sector_name:
            return
        previous = self._previous_sector_day(sector_name)
        prev_close = float(previous.get("close") or 0)
        change = ((price - prev_close) / prev_close * 100) if prev_close > 0 else None
        self.sector_live[sector_name] = {
            "sector": sector_name,
            "security_id": str(security_id),
            "segment": "IDX_I",
            "price": round(float(price), 2),
            "previous_close": prev_close,
            "change": round(change, 2) if change is not None else None,
            "updated_at": timestamp.isoformat(),
        }

    def _passes_sector_filter(self, symbol: str, side: str) -> bool:
        if not self.settings.use_sector_filter:
            return True
        sectors = sectors_for_symbol(symbol)
        if not sectors:
            return False
        top = self._top_sectors(side)
        return any(sector in top for sector in sectors)

    def _top_sectors(self, side: str) -> set[str]:
        rows = [
            (sector, payload.get("change"))
            for sector, payload in self.sector_live.items()
            if payload.get("change") is not None
        ]
        rows.sort(key=lambda item: item[1], reverse=(side == "long"))
        return {sector for sector, _change in rows[: self.settings.top_sector_count]}

    def sector_rankings(self) -> list[dict[str, Any]]:
        rows = []
        cache = self._premarket_cache()
        cached = cache.get("sectors") or {}
        for sector_name, instrument in self.sector_instruments.items():
            live = self.sector_live.get(sector_name) or {}
            previous_day = (cached.get(sector_name) or {}).get("previous_day") or {}
            rows.append(
                {
                    "sector": sector_name,
                    "security_id": instrument.security_id,
                    "segment": "IDX_I",
                    "price": live.get("price") or 0,
                    "previous_close": live.get("previous_close") or previous_day.get("close") or 0,
                    "change": live.get("change") if live else None,
                    "updated_at": live.get("updated_at") or "",
                    "cached": bool(previous_day.get("close")),
                }
            )
        rows.sort(key=lambda item: item["change"] if item["change"] is not None else -999999, reverse=True)
        return rows

    def _open_positions_for_symbol(self, symbol: str) -> list[Position]:
        return [p for p in self.positions.values() if p.symbol == symbol and p.status == "OPEN"]

    def _has_open_position(self, symbol: str, side: str | None = None) -> bool:
        return any(
            p.symbol == symbol
            and p.status == "OPEN"
            and (side is None or p.side == side)
            for p in self.positions.values()
        )

    def event(self, kind: str, message: str) -> None:
        self.events.insert(0, {"time": datetime.now(IST).isoformat(), "kind": kind, "message": message})
        del self.events[200:]

    async def _snapshot_worker(self) -> None:
        while self.running:
            try:
                self._refresh_snapshot_cache()
            except Exception as exc:
                self.event("ERROR", f"Snapshot refresh failed: {exc}")
            await asyncio.sleep(1.0)

    def _refresh_snapshot_cache(self) -> dict[str, Any]:
        snapshot = self._build_snapshot()
        self.snapshot_cache = snapshot
        self.snapshot_cache_ts = time.monotonic()
        return snapshot

    def snapshot(self, fresh: bool = False) -> dict[str, Any]:
        if fresh or not self.running or not self.snapshot_cache:
            return self._refresh_snapshot_cache()
        if time.monotonic() - self.snapshot_cache_ts > 2.5:
            return self._refresh_snapshot_cache()
        return dict(self.snapshot_cache)

    def _build_snapshot(self) -> dict[str, Any]:
        with self.lock:
            self._refresh_external_worker_state()
            effective_locks = sorted(self.locked_symbols | self.external_locked_symbols)
            symbols = sorted(self.instruments_by_symbol)
            latest = []
            for symbol in symbols:
                instrument = self.instruments_by_symbol[symbol]
                latest.append(
                    {
                        "symbol": symbol,
                        "security_id": instrument.security_id,
                        "sector": instrument.sector,
                        "price": self.candles.latest_price(symbol),
                        "candles_1m": len(self.candles.closed_candles(symbol, 1)),
                        "candles_5m": len(self.candles.closed_candles(symbol, 5)),
                        "locked": symbol in effective_locks,
                    }
                )
            premarket = dict(self.premarket_status)
            if self.enable_reconcile_workers and not premarket.get("running"):
                premarket["summary"] = self._premarket_cache_summary_snapshot()
            return {
                "running": self.running,
                "market_connected": self.market_connected,
                "market_connecting": self.market_connecting,
                "order_connected": self.order_connected,
                "order_connecting": self.order_connecting,
                "order_last_error": self.order_last_error,
                "order_reconnects": self.order_reconnects,
                "last_error": self.last_error,
                "last_tick_age_seconds": round(time.time() - self.last_tick_ts, 1) if self.last_tick_ts else None,
                "tick_queue_size": self.tick_queue.qsize() if self.tick_queue else 0,
                "tick_queue_maxsize": self.tick_queue.maxsize if self.tick_queue else TICK_QUEUE_MAXSIZE,
                "dropped_ticks": self.dropped_ticks,
                "execution_queue_size": self.execution_queue.qsize() if self.execution_queue else 0,
                "execution_workers": sum(1 for task in self.execution_tasks if not task.done()),
                "pending_actions": sorted(self.pending_actions),
                "settings": self.settings.__dict__,
                "credentials_present": bool(self.credentials.get("client_id") and self.credentials.get("access_token")),
                "credential_client_id": self.credentials.get("client_id") or "",
                "long_symbols": self.long_symbols,
                "short_symbols": self.short_symbols,
                "universe_symbols": sorted(self.universe_symbols),
                "universe_count": len(self.universe_symbols),
                "resolved_count": len(self.instruments_by_symbol),
                "latest": latest,
                "sectors": self.sector_rankings(),
                "top_long_sectors": sorted(self._top_sectors("long")),
                "top_short_sectors": sorted(self._top_sectors("short")),
                "positions": [p.as_dict() for p in self.positions.values()],
                "events": self.events[:80],
                "premarket": premarket,
                "reconcile": self.reconcile_status,
                "broker_reconcile": self.broker_reconcile_status,
                "locked_symbols": effective_locks,
                "entries_blocked_until_reconcile": self.entries_blocked_until_reconcile,
                "ledger_file": str(TRADE_LEDGER_FILE),
            }


def chunked(values: list[Any], size: int) -> list[list[Any]]:
    return [values[i : i + size] for i in range(0, len(values), size)]
