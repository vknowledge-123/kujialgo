from __future__ import annotations

import asyncio
import os
import signal
import time
from datetime import datetime
from typing import Any

from .candles import IST
from .config import ENGINE_COMMANDS_FILE, ENGINE_COMMAND_POLL_SECONDS, ENGINE_COMMAND_STATE_FILE, ENGINE_SNAPSHOT_INTERVAL_SECONDS, RUNTIME_SNAPSHOT_FILE
from .engine import DhanAlgoEngine
from .storage import read_json, read_jsonl, write_json


class EngineService:
    def __init__(self):
        self.engine = DhanAlgoEngine(enable_reconcile_workers=False, enable_snapshot_worker=False)
        state = read_json(ENGINE_COMMAND_STATE_FILE, {"processed": []})
        self.processed: set[str] = set(state.get("processed") or [])
        self.stopping = asyncio.Event()

    async def run(self) -> None:
        self._install_signal_handlers()
        await asyncio.gather(self._command_loop(), self._snapshot_loop(), self.stopping.wait())
        if self.engine.running:
            await self.engine.stop()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stopping.set)
            except NotImplementedError:
                pass

    async def _command_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                for command in read_jsonl(ENGINE_COMMANDS_FILE):
                    command_id = str(command.get("id") or "")
                    if not command_id or command_id in self.processed:
                        continue
                    try:
                        await self._handle_command(command)
                    except Exception as exc:
                        self.engine.event("ERROR", f"Engine command {command_id} failed: {exc}")
                    self._mark_processed(command_id)
            except Exception as exc:
                self.engine.event("ERROR", f"Engine command loop failed: {exc}")
            await asyncio.sleep(ENGINE_COMMAND_POLL_SECONDS)

    def _mark_processed(self, command_id: str) -> None:
        self.processed.add(command_id)
        write_json(ENGINE_COMMAND_STATE_FILE, {"processed": sorted(self.processed)[-1000:]})

    async def _handle_command(self, command: dict[str, Any]) -> None:
        action = str(command.get("action") or "").lower()
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        if action == "config":
            self.engine._load_state()
            self.engine.configure(payload)
            self.engine.event("INFO", "Config command applied by engine service.")
        elif action == "start":
            self.engine._load_state()
            if payload:
                self.engine.configure(payload)
            await self.engine.start()
        elif action == "stop":
            await self.engine.stop()
        else:
            self.engine.event("WARN", f"Unknown engine command ignored: {action}")

    async def _snapshot_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                snapshot = self.engine.snapshot(fresh=True)
                snapshot["service"] = "engine"
                snapshot["service_pid"] = os.getpid()
                snapshot["published_at"] = datetime.now(IST).isoformat()
                snapshot["published_epoch"] = time.time()
                snapshot["published_monotonic"] = time.monotonic()
                write_json(RUNTIME_SNAPSHOT_FILE, snapshot)
            except Exception as exc:
                fallback = {
                    "running": False,
                    "market_connected": False,
                    "order_connected": False,
                    "last_error": f"Engine snapshot publish failed: {exc}",
                    "published_at": datetime.now(IST).isoformat(),
                    "published_epoch": time.time(),
                    "published_monotonic": time.monotonic(),
                }
                write_json(RUNTIME_SNAPSHOT_FILE, fallback)
            await asyncio.sleep(ENGINE_SNAPSHOT_INTERVAL_SECONDS)


def main() -> None:
    asyncio.run(EngineService().run())


if __name__ == "__main__":
    main()
