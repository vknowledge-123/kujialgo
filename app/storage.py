import json
import threading
from pathlib import Path
from typing import Any

from .config import DATA_DIR

_LOCK = threading.RLock()


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def read_json(path: Path, default: Any) -> Any:
    ensure_data_dir()
    if not path.exists():
        return default
    with _LOCK:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return default


def write_json(path: Path, payload: Any) -> None:
    ensure_data_dir()
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    with _LOCK:
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(path)


def append_jsonl(path: Path, payload: Any) -> None:
    ensure_data_dir()
    line = json.dumps(payload, ensure_ascii=True, sort_keys=True)
    with _LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def read_jsonl(path: Path) -> list[Any]:
    ensure_data_dir()
    if not path.exists():
        return []
    rows = []
    with _LOCK:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def read_jsonl_from(path: Path, offset: int = 0) -> tuple[list[Any], int]:
    ensure_data_dir()
    if not path.exists():
        return [], 0
    rows = []
    with _LOCK:
        size = path.stat().st_size
        if offset < 0 or offset > size:
            offset = 0
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            handle.seek(offset)
            for line in handle:
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
            return rows, handle.tell()
