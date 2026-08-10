from __future__ import annotations

import asyncio
import os
import signal
from datetime import datetime
from typing import Any

from .candles import IST
from .config import (
    RECONCILED_CANDLES_FILE,
    RECONCILE_BROKER_INTERVAL_SECONDS,
    RECONCILE_CANDLE_INTERVAL_SECONDS,
    RECONCILE_COMMAND_POLL_SECONDS,
    RECONCILE_COMMAND_STATE_FILE,
    RECONCILE_COMMANDS_FILE,
    RECONCILE_SNAPSHOT_FILE,
)
from .dhan_api import DhanClient
from .engine import DhanAlgoEngine
from .storage import read_json, read_jsonl_from, write_json


class ReconcileService:
    def __init__(self):
        self.engine = DhanAlgoEngine(enable_reconcile_workers=False, enable_snapshot_worker=False)
        state = read_json(RECONCILE_COMMAND_STATE_FILE, {})
        self.processed: set[str] = set(state.get("processed") or [])
        self.command_offset = self._initial_command_offset(state)
        self.stopping = asyncio.Event()
        self.next_broker_run = 0.0
        self.next_candle_run = 0.0

    async def run(self) -> None:
        self._install_signal_handlers()
        await asyncio.gather(self._command_loop(), self._periodic_loop(), self._snapshot_loop(), self.stopping.wait())

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stopping.set)
            except NotImplementedError:
                pass

    def _reload_runtime_state(self) -> bool:
        self.engine._load_state()
        self.engine._load_ledger()
        self.engine._load_positions_from_ledger()
        self.engine._resolve_watchlists()
        if self.engine.credentials.get("client_id") and self.engine.credentials.get("access_token"):
            self.engine.client = DhanClient(self.engine.credentials["client_id"], self.engine.credentials["access_token"])
            return True
        return False

    async def _command_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                commands, new_offset = read_jsonl_from(RECONCILE_COMMANDS_FILE, self.command_offset)
                for command in commands:
                    command_id = str(command.get("id") or "")
                    if not command_id or command_id in self.processed:
                        continue
                    try:
                        await self._handle_command(command)
                    except Exception as exc:
                        self.engine.event("ERROR", f"Reconcile command {command_id} failed: {exc}")
                    self._mark_processed(command_id)
                self.command_offset = new_offset
                self._save_command_state()
            except Exception as exc:
                self.engine.event("ERROR", f"Reconcile command loop failed: {exc}")
            await asyncio.sleep(RECONCILE_COMMAND_POLL_SECONDS)

    def _mark_processed(self, command_id: str) -> None:
        self.processed.add(command_id)
        self._save_command_state()

    def _initial_command_offset(self, state: dict[str, Any]) -> int:
        if "offset" in state:
            return int(state.get("offset") or 0)
        try:
            return RECONCILE_COMMANDS_FILE.stat().st_size
        except OSError:
            return 0

    def _save_command_state(self) -> None:
        write_json(
            RECONCILE_COMMAND_STATE_FILE,
            {
                "offset": self.command_offset,
                "processed": sorted(self.processed)[-200:],
            },
        )

    async def _handle_command(self, command: dict[str, Any]) -> None:
        action = str(command.get("action") or "").lower()
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        if action == "config":
            self._reload_runtime_state()
            self.engine.event("INFO", "Config command applied by reconcile service.")
        elif action == "premarket-cache":
            if self._reload_runtime_state():
                await self.engine.run_premarket_cache(force=bool(payload.get("force", False)))
            else:
                self.engine.premarket_status = {"running": False, "message": "Credentials missing", "progress": 0}
        elif action == "broker-reconcile":
            if self._reload_runtime_state():
                await self._run_broker_reconcile()
        elif action == "reconcile-symbol":
            if self._reload_runtime_state():
                symbol = str(payload.get("symbol") or "")
                try:
                    await self.engine.reconcile_missing_candles(symbol)
                    self._export_reconciled_candles()
                except Exception as exc:
                    self.engine.reconcile_status = {
                        "running": False,
                        "message": f"Manual reconcile failed for {symbol}: {exc}",
                        "last_run": datetime.now(IST).isoformat(),
                        "last_missing": {},
                    }
        else:
            self.engine.event("WARN", f"Unknown reconcile command ignored: {action}")

    async def _periodic_loop(self) -> None:
        while not self.stopping.is_set():
            if self.engine.cache_task and not self.engine.cache_task.done():
                await asyncio.sleep(1)
                continue
            loop_time = asyncio.get_running_loop().time()
            if self._reload_runtime_state():
                if loop_time >= self.next_candle_run:
                    await self._run_candle_reconcile()
                    self.next_candle_run = loop_time + RECONCILE_CANDLE_INTERVAL_SECONDS
                if loop_time >= self.next_broker_run:
                    await self._run_broker_reconcile()
                    self.next_broker_run = loop_time + RECONCILE_BROKER_INTERVAL_SECONDS
            await asyncio.sleep(1)

    async def _run_candle_reconcile(self) -> None:
        if not self.engine.instruments_by_symbol:
            return
        try:
            summary = await self.engine.reconcile_all_missing_candles()
            self.engine.reconcile_status = {
                "running": False,
                "message": "Auto reconcile complete",
                "last_run": datetime.now(IST).isoformat(),
                "last_missing": summary,
            }
            self._export_reconciled_candles()
        except Exception as exc:
            self.engine.reconcile_status = {
                "running": False,
                "message": f"Auto reconcile failed: {exc}",
                "last_run": datetime.now(IST).isoformat(),
                "last_missing": {},
            }
            self.engine.event("WARN", self.engine.reconcile_status["message"])

    async def _run_broker_reconcile(self) -> None:
        try:
            await self.engine.reconcile_broker_state()
        except Exception as exc:
            self.engine.broker_reconcile_status = {
                **self.engine.broker_reconcile_status,
                "running": False,
                "message": f"Broker reconcile failed: {exc}; trading continues with existing symbol locks.",
                "last_run": datetime.now(IST).isoformat(),
                "entries_blocked_until_reconcile": False,
            }
            self.engine.event("WARN", self.engine.broker_reconcile_status["message"])

    async def _snapshot_loop(self) -> None:
        while not self.stopping.is_set():
            self._write_snapshot()
            await asyncio.sleep(1)

    def _write_snapshot(self) -> None:
        broker = dict(self.engine.broker_reconcile_status)
        locks = sorted(set(self.engine.locked_symbols) | set(broker.get("locked_symbols") or []))
        broker["locked_symbols"] = locks
        write_json(
            RECONCILE_SNAPSHOT_FILE,
            {
                "service": "reconcile",
                "service_pid": os.getpid(),
                "published_at": datetime.now(IST).isoformat(),
                "premarket": self.engine.premarket_status,
                "reconcile": self.engine.reconcile_status,
                "broker_reconcile": broker,
                "locked_symbols": locks,
                "events": self.engine.events[:80],
            },
        )

    def _export_reconciled_candles(self) -> None:
        symbols: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for (symbol, timeframe), candles in self.engine.candles.closed.items():
            symbol_rows = symbols.setdefault(symbol, {})
            symbol_rows[str(timeframe)] = [candle.as_dict() for candle in candles]
        write_json(
            RECONCILED_CANDLES_FILE,
            {
                "updated_at": datetime.now(IST).isoformat(),
                "symbols": symbols,
            },
        )


def main() -> None:
    asyncio.run(ReconcileService().run())


if __name__ == "__main__":
    main()
