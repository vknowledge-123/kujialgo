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
