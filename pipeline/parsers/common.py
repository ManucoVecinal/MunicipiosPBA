# -*- coding: utf-8 -*-
"""
Helpers comunes para parsers de SITECO.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional


_AMOUNT_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2})")


def normalize_name(text: Optional[str]) -> str:
    if text is None:
        return ""
    return " ".join(text.strip().split())


def normalize_key(text: Optional[str]) -> str:
    base = normalize_name(text)
    base = unicodedata.normalize("NFKD", base)
    base = base.encode("ascii", "ignore").decode("ascii")
    return base.lower()


def parse_amount_ar(value: str) -> float:
    if value is None:
        raise ValueError("Valor vacio.")
    s = value.strip()
    if not s:
        raise ValueError("Valor vacio.")
    s = s.replace(".", "").replace(",", ".")
    return float(s)


def find_amounts(text: str) -> list[str]:
    return _AMOUNT_RE.findall(text or "")
