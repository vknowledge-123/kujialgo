import asyncio
import json
import struct
import threading
import time
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import websockets

from .candles import CandleStore, IST, expected_starts_for_day, to_ist
from .config import (
    DHAN_FEED_URL,
    DHAN_ORDER_UPDATE_URL,
    MAX_INSTRUMENTS_PER_CONNECTION,
    MAX_MARKET_FEED_CONNECTIONS,
    MAX_SUBSCRIBE_BATCH,
    PREMARKET_FILE,
    PREMARKET_REPORT_FILE,
    STATE_FILE,
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
FAILED_STATUSES = {"REJECTED", "CANCELLED", "EXPIRED", "FAILED"}
PENDING_STATUSES = {"TRANSIT", "PENDING"}


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
    def __init__(self):
        self.resolver = InstrumentResolver()
        self.client: DhanClient | None = None
        self.settings = StrategySettings()
        self.candles = CandleStore()
        self.long_evaluator = LongStrategyEvaluator()
        self.running = False
        self.market_connected = False
        self.order_connected = False
        self.order_last_error = ""
        self.order_reconnects = 0
        self.last_error = ""
        self.last_tick_ts = 0.0
        self.feed_generation = 0
        self.loop: asyncio.AbstractEventLoop | None = None
        self.tick_queue: asyncio.Queue[tuple[str, str, float, int | None, datetime]] | None = None
        self.feed_threads: list[threading.Thread] = []
        self.order_task: asyncio.Task | None = None
        self.tick_task: asyncio.Task | None = None
        self.cache_task: asyncio.Task | None = None
        self.reconcile_task: asyncio.Task | None = None
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
        self.events: list[dict[str, Any]] = []
        self.premarket_status: dict[str, Any] = {"running": False, "message": "Not started", "progress": 0}
        self.reconcile_status: dict[str, Any] = {"running": False, "message": "Not started", "last_run": "", "last_missing": {}}
        self.lock = threading.RLock()
        self._load_state()

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

    def _handle_auth_failure(self, source: str, exc: Exception) -> None:
        self.last_error = f"{source}: Dhan credentials invalid or expired. Update client ID/access token and restart algo."
        self.event("ERROR", self.last_error)
        self.running = False
        self.feed_generation += 1
        self.client = None
        self.market_connected = False
        self.order_connected = False
        self.reconcile_status = {
            "running": False,
            "message": self.last_error,
            "last_run": datetime.now(IST).isoformat(),
            "last_missing": {},
        }
        current = None
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in (self.tick_task, self.order_task, self.reconcile_task):
            if task and task is not current and not task.done():
                task.cancel()

    def configure(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
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
                self.long_symbols = extract_symbols(payload.get("long_text") or "", self.universe_symbols)
            if "short_text" in payload:
                self.short_symbols = extract_symbols(payload.get("short_text") or "", self.universe_symbols)
            self._resolve_watchlists()
            self.save_state()
        return self.snapshot()

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
            return self.snapshot()
        self.loop = asyncio.get_running_loop()
        self.tick_queue = asyncio.Queue(maxsize=20000)
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
        self.running = True
        self.feed_generation += 1
        self.tick_task = asyncio.create_task(self._tick_worker())
        self.order_task = asyncio.create_task(self._order_update_worker())
        self.reconcile_task = asyncio.create_task(self._auto_reconcile_worker())
        self._start_market_feed_threads(self.feed_generation)
        self.event("INFO", f"Algo started with {len(self.instruments_by_symbol)} stocks and {len(self.sector_instruments)} sector indexes on {self.settings.timeframe}m.")
        return self.snapshot()

    async def stop(self) -> dict[str, Any]:
        self.running = False
        self.feed_generation += 1
        self.market_connected = False
        self.order_connected = False
        for task in (self.tick_task, self.order_task, self.reconcile_task):
            if task:
                task.cancel()
        self.event("INFO", "Algo stopped.")
        return self.snapshot()

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
            self.last_error = ""
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
            self.last_error = f"Dhan market WebSocket error: {error}"

        def on_close(_ws, code, reason):
            self.market_connected = False
            if generation == self.feed_generation and self.running:
                self.last_error = f"Dhan market WebSocket closed: {code or ''} {reason or ''}".strip()

        backoff = 2
        while generation == self.feed_generation and self.running:
            ws = websocket_module.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            try:
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                self.market_connected = False
                self.last_error = f"Dhan market WebSocket stopped: {exc}"
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
        try:
            self.tick_queue.put_nowait((segment, security_id, price, volume, timestamp))
        except asyncio.QueueFull:
            self.event("WARN", "Tick queue is full; dropping latest tick.")

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
            await self._evaluate_tick(instrument, price)
            for candle in closed:
                await self._evaluate_closed_candle(instrument, candle)

    async def _evaluate_tick(self, instrument: Instrument, price: float) -> None:
        symbol = instrument.symbol
        if symbol not in self.long_symbols or self._has_open_position(symbol):
            return
        if not self._passes_sector_filter(symbol, "long"):
            return
        candles = self.candles.all_candles(symbol, self.settings.timeframe)
        signal = self.long_evaluator.evaluate_tick_entry(symbol, price, candles, self.settings)
        if signal:
            await self._enter_position(instrument, signal)

    async def _evaluate_closed_candle(self, instrument: Instrument, candle: Candle) -> None:
        if candle.timeframe != self.settings.timeframe:
            return
        symbol = instrument.symbol
        candles = self.candles.closed_candles(symbol, self.settings.timeframe)
        for position in list(self.positions.values()):
            if position.symbol == symbol and position.status == "OPEN":
                reason = self.long_evaluator.evaluate_exit(position, candles, self.settings)
                if reason:
                    await self._exit_position(position, reason)
        if symbol not in self.long_symbols or self._has_open_position(symbol):
            return
        previous_day = self._previous_day(symbol)
        baseline = self._baseline(symbol, self.settings.timeframe)
        for event in self.long_evaluator.on_closed_candle(symbol, candles, previous_day, baseline, self.settings):
            self.event(event.get("type", "INFO"), f"{symbol} {event.get('strategy')}: {event.get('reason')}")
            if event.get("type") == "ENTRY" and self._passes_sector_filter(symbol, "long"):
                await self._enter_position(instrument, event)

    async def _enter_position(self, instrument: Instrument, signal: dict[str, Any]) -> None:
        entry = float(signal["entry_price"])
        stop, target = setup_stop(entry, signal["stop_candle"], self.settings)
        quantity, sizing_reason = calculate_quantity(entry, stop, self.settings)
        if quantity < 1:
            self.event("WARN", f"{instrument.symbol} entry skipped: calculated quantity is 0 ({sizing_reason}).")
            return
        correlation_id = f"KJ{uuid4().hex[:18]}"
        order = await self._place_with_retry(instrument, "BUY", quantity, correlation_id)
        status = order.get("status", "")
        if str(status).upper() not in TRADED_STATUSES:
            self.event("ERROR", f"{instrument.symbol} entry not opened because order status is {status or 'UNKNOWN'}.")
            return
        position = Position(
            symbol=instrument.symbol,
            security_id=instrument.security_id,
            side="LONG",
            strategy=signal["strategy"],
            entry_price=entry,
            quantity=quantity,
            stop_loss=stop,
            target=target,
            opened_at=datetime.now(IST).isoformat(),
            order_id=str(order.get("order_id") or ""),
            last_price=entry,
            meta={"entry_order_status": status, "reason": signal.get("reason"), "sizing": sizing_reason},
        )
        self.positions[f"{instrument.symbol}:LONG"] = position
        self.event("ENTRY", f"{instrument.symbol} {signal['strategy']} BUY {quantity} at {entry:.2f}, SL {stop:.2f}, target {target:.2f} ({sizing_reason})")

    async def _exit_position(self, position: Position, reason: str) -> None:
        if position.status != "OPEN":
            return
        order = await self._place_with_retry(
            Instrument(position.symbol, position.security_id),
            "SELL",
            position.quantity,
            f"KJX{uuid4().hex[:17]}",
        )
        position.status = "CLOSED"
        position.exit_order_id = str(order.get("order_id") or "")
        position.exit_reason = reason
        self.event("EXIT", f"{position.symbol} exit {reason} at approx {position.last_price or 0:.2f}")

    async def _place_with_retry(self, instrument: Instrument, transaction_type: str, quantity: int, correlation_id: str) -> dict[str, Any]:
        if self.settings.dry_run:
            return {"order_id": f"DRY-{uuid4().hex[:10]}", "status": "TRADED", "attempts": 1}
        assert self.client is not None
        last_result: dict[str, Any] = {}
        for attempt in range(1, 4):
            result = await self.client.place_market_order(instrument.security_id, transaction_type, quantity, f"{correlation_id}{attempt}")
            order_id = str(result.get("orderId") or (result.get("data") or {}).get("orderId") or "")
            status = str(result.get("orderStatus") or (result.get("data") or {}).get("orderStatus") or "").upper()
            last_result = {"order_id": order_id, "status": status, "attempts": attempt, "raw": result}
            final_status = await self._wait_order_status(order_id, status)
            last_result["status"] = final_status
            if final_status in TRADED_STATUSES:
                return last_result
            if order_id and final_status in PENDING_STATUSES:
                try:
                    await self.client.cancel_order(order_id)
                except Exception as exc:
                    self.event("WARN", f"Cancel before retry failed for {instrument.symbol}: {exc}")
            await asyncio.sleep(0.25 * attempt)
        self.event("ERROR", f"{instrument.symbol} {transaction_type} failed after 3 attempts: {last_result.get('status')}")
        return last_result

    async def _wait_order_status(self, order_id: str, initial_status: str) -> str:
        if not order_id:
            return initial_status or "UNKNOWN"
        deadline = time.monotonic() + 2.5
        status = initial_status or "UNKNOWN"
        while time.monotonic() < deadline:
            update = self.order_updates.get(order_id)
            if update:
                status = str(update.get("Status") or update.get("OrderStatus") or update.get("orderStatus") or status).upper()
                if status in TRADED_STATUSES | FAILED_STATUSES:
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
            try:
                async with websockets.connect(
                    DHAN_ORDER_UPDATE_URL,
                    ping_interval=None,
                    open_timeout=10,
                    close_timeout=1,
                ) as ws:
                    await ws.send(json.dumps({"LoginReq": {"MsgCode": 42, "ClientId": self.credentials["client_id"], "Token": self.credentials["access_token"]}, "UserType": "SELF"}))
                    self.order_connected = True
                    self.order_last_error = ""
                    self.order_reconnects = 0
                    async for message in ws:
                        for payload in _json_objects_from_message(message):
                            if not isinstance(payload, dict):
                                continue
                            if payload.get("errorCode") == "DH-901" or payload.get("errorType") == "Invalid_Authentication":
                                self._handle_auth_failure("Dhan order WebSocket", DhanAuthenticationError(json.dumps(payload)[:300]))
                                return
                            data = payload.get("Data") or payload.get("data") or payload
                            if not isinstance(data, dict):
                                continue
                            order_no = str(data.get("OrderNo") or data.get("orderId") or data.get("OrderId") or "")
                            if order_no:
                                self.order_updates[order_no] = data
            except asyncio.CancelledError:
                break
            except DhanAuthenticationError as exc:
                self._handle_auth_failure("Dhan order WebSocket", exc)
                break
            except Exception as exc:
                if _is_auth_error(exc):
                    self._handle_auth_failure("Dhan order WebSocket", exc)
                    break
                self.order_connected = False
                self.order_reconnects += 1
                self.order_last_error = f"Order update socket reconnecting: {exc}"
                if self.order_reconnects == 1 or self.order_reconnects % 10 == 0:
                    self.event("INFO", self.order_last_error)
                await asyncio.sleep(min(30, 3 + self.order_reconnects))

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
                rows = await self.client.intraday(instrument.security_id, timeframe, min(missing), now)
                self.candles.seed_history(instrument, timeframe, rows)
            result[str(timeframe)] = len(missing)
        return result

    def _previous_day(self, symbol: str) -> dict[str, float]:
        cache = read_json(PREMARKET_FILE, {"symbols": {}})
        return ((cache.get("symbols") or {}).get(symbol) or {}).get("previous_day") or {}

    def _baseline(self, symbol: str, timeframe: int) -> list[int]:
        cache = read_json(PREMARKET_FILE, {"symbols": {}})
        return (((cache.get("symbols") or {}).get(symbol) or {}).get("baseline") or {}).get(str(timeframe)) or []

    def _premarket_cache_summary_snapshot(self) -> dict[str, Any]:
        universe = sorted(self.universe_symbols or set(self.long_symbols + self.short_symbols))
        if universe:
            resolved, _missing = self.resolver.resolve(universe)
        else:
            resolved = list(self.instruments_by_symbol.values())
        cache = read_json(PREMARKET_FILE, {"symbols": {}, "sectors": {}, "errors": {}})
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
        cache = read_json(PREMARKET_FILE, {"sectors": {}})
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
        cache = read_json(PREMARKET_FILE, {"sectors": {}})
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

    def _has_open_position(self, symbol: str) -> bool:
        return any(p.symbol == symbol and p.status == "OPEN" for p in self.positions.values())

    def event(self, kind: str, message: str) -> None:
        self.events.insert(0, {"time": datetime.now(IST).isoformat(), "kind": kind, "message": message})
        del self.events[200:]

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
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
                    }
                )
            premarket = dict(self.premarket_status)
            if not premarket.get("running"):
                premarket["summary"] = self._premarket_cache_summary_snapshot()
            return {
                "running": self.running,
                "market_connected": self.market_connected,
                "order_connected": self.order_connected,
                "order_last_error": self.order_last_error,
                "order_reconnects": self.order_reconnects,
                "last_error": self.last_error,
                "last_tick_age_seconds": round(time.time() - self.last_tick_ts, 1) if self.last_tick_ts else None,
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
            }


def chunked(values: list[Any], size: int) -> list[list[Any]]:
    return [values[i : i + size] for i in range(0, len(values), size)]
