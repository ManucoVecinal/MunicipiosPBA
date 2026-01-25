from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class LogEvent:
    event: str
    detail: dict[str, Any]
    ts: float


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def log_event(log_path: str, event: str, detail: dict[str, Any]) -> None:
    _ensure_dir(os.path.dirname(log_path))
    payload = LogEvent(event=event, detail=detail, ts=time.time())
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(payload), ensure_ascii=True) + "\n")
