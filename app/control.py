from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from uuid import uuid4

from .candles import IST
from .config import (
    ENGINE_COMMANDS_FILE,
    PREMARKET_REPORT_FILE,
    RECONCILE_COMMANDS_FILE,
    RECONCILE_SNAPSHOT_FILE,
    RUNTIME_SNAPSHOT_FILE,
    RUNTIME_SNAPSHOT_STALE_SECONDS,
    STATE_FILE,
    TICK_QUEUE_MAXSIZE,
    TRADE_LEDGER_FILE,
)
from .storage import append_jsonl, read_json, write_json
from .strategy import StrategySettings
from .symbols import extract_symbols


def now_iso() -> str:
    return datetime.now(IST).isoformat()


def save_runtime_config(payload: dict[str, Any]) -> dict[str, Any]:
    state = read_json(STATE_FILE, {})
    if payload.get("credentials"):
        creds = payload["credentials"]
        client_id = str(creds.get("client_id") or "").strip()
        access_token = str(creds.get("access_token") or "").strip()
        if client_id and access_token:
            state["credentials"] = {"client_id": client_id, "access_token": access_token}
    if "settings" in payload:
        current = StrategySettings.from_payload(state.get("settings") or {})
        merged = dict(current.__dict__)
        enabled = dict(current.enabled)
        incoming = payload.get("settings") or {}
        incoming_enabled = incoming.get("enabled")
        merged.update(incoming)
        if incoming_enabled is not None:
            enabled.update(incoming_enabled)
            merged["enabled"] = enabled
        state["settings"] = StrategySettings.from_payload(merged).__dict__
    if "universe_text" in payload:
        state["universe_symbols"] = extract_symbols(payload.get("universe_text") or "")
    if "long_text" in payload:
        state["long_symbols"] = extract_symbols(payload.get("long_text") or "")
    if "short_text" in payload:
        state["short_symbols"] = extract_symbols(payload.get("short_text") or "")
    state["updated_at"] = now_iso()
    write_json(STATE_FILE, state)
    return state


def queue_command(target: str, action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    command = {
        "id": uuid4().hex,
        "target": target,
        "action": action,
        "payload": payload or {},
        "created_at": now_iso(),
    }
    path = ENGINE_COMMANDS_FILE if target == "engine" else RECONCILE_COMMANDS_FILE
    append_jsonl(path, command)
    return command


def fallback_snapshot(message: str = "Engine service has not published a snapshot yet.") -> dict[str, Any]:
    state = read_json(STATE_FILE, {})
    settings = StrategySettings.from_payload(state.get("settings") or {}).__dict__
    return {
        "running": False,
        "market_connected": False,
        "market_connecting": False,
        "order_connected": False,
        "order_connecting": False,
        "order_last_error": "",
        "order_reconnects": 0,
        "last_error": message,
        "last_tick_age_seconds": None,
        "tick_queue_size": 0,
        "tick_queue_maxsize": TICK_QUEUE_MAXSIZE,
        "dropped_ticks": 0,
        "execution_queue_size": 0,
        "execution_workers": 0,
        "pending_actions": [],
        "settings": settings,
        "credentials_present": bool((state.get("credentials") or {}).get("client_id") and (state.get("credentials") or {}).get("access_token")),
        "credential_client_id": (state.get("credentials") or {}).get("client_id") or "",
        "long_symbols": state.get("long_symbols") or [],
        "short_symbols": state.get("short_symbols") or [],
        "universe_symbols": sorted(state.get("universe_symbols") or []),
        "universe_count": len(state.get("universe_symbols") or []),
        "resolved_count": 0,
        "latest": [],
        "sectors": [],
        "top_long_sectors": [],
        "top_short_sectors": [],
        "positions": [],
        "events": [],
        "premarket": {"running": False, "message": "Not started", "progress": 0},
        "reconcile": {"running": False, "message": "Not started", "last_run": "", "last_missing": {}},
        "broker_reconcile": {
            "running": False,
            "message": "Not checked",
            "last_run": "",
            "mismatches": [],
            "pending_orders": [],
            "failed_orders": [],
            "locked_symbols": [],
            "entries_blocked_until_reconcile": False,
        },
        "locked_symbols": [],
        "entries_blocked_until_reconcile": False,
        "ledger_file": str(TRADE_LEDGER_FILE),
    }


def read_status_snapshot() -> dict[str, Any]:
    defaults = fallback_snapshot()
    snapshot = read_json(RUNTIME_SNAPSHOT_FILE, {})
    if not isinstance(snapshot, dict) or not snapshot:
        snapshot = defaults
    else:
        snapshot = {**defaults, **snapshot}
    published_at = float(snapshot.get("published_epoch") or 0)
    age = time.time() - published_at if published_at else 0
    if published_at and age > RUNTIME_SNAPSHOT_STALE_SECONDS:
        snapshot = {
            **snapshot,
            "running": False,
            "market_connected": False,
            "market_connecting": False,
            "order_connected": False,
            "order_connecting": False,
            "last_error": f"Engine snapshot stale for {age:.1f}s. Check kujialgo-engine service.",
        }
    reconcile = read_json(RECONCILE_SNAPSHOT_FILE, {})
    if isinstance(reconcile, dict) and reconcile:
        for key in ("premarket", "reconcile", "broker_reconcile"):
            if isinstance(reconcile.get(key), dict):
                snapshot[key] = reconcile[key]
        if reconcile.get("locked_symbols") is not None:
            locks = sorted(set(snapshot.get("locked_symbols") or []) | set(reconcile.get("locked_symbols") or []))
            snapshot["locked_symbols"] = locks
            broker = dict(snapshot.get("broker_reconcile") or {})
            broker["locked_symbols"] = locks
            snapshot["broker_reconcile"] = broker
    report = read_json(PREMARKET_REPORT_FILE, {})
    if isinstance(report, dict) and report.get("summary"):
        premarket = dict(snapshot.get("premarket") or {})
        premarket.setdefault("summary", report["summary"])
        snapshot["premarket"] = premarket
    return snapshot
