# -*- coding: utf-8 -*-
"""
Helpers comunes: hashing y timestamps.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def compute_sha256(pdf_bytes: bytes) -> str:
    """
    Calcula el SHA256 de un archivo en bytes.
    """
    h = hashlib.sha256()
    h.update(pdf_bytes)
    return h.hexdigest()


def utc_now_iso() -> str:
    """
    Timestamp en formato ISO 8601 (UTC, con 'Z').
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
