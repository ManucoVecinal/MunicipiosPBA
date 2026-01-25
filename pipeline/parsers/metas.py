# -*- coding: utf-8 -*-
"""
Parser de metas desde el PDF usando pdfplumber (por posicion).
"""
from __future__ import annotations

import re
from io import BytesIO
from typing import Dict, List, Optional

from .common import normalize_key, normalize_name, parse_amount_ar


_SECTION_START = "evolucion de las principales metas de programas"
_SECTION_END_KEYWORDS = [
    "r.a.f.a.m.",
    "hoja:",
    "estado de situacion patrimonial",
    "movimientos de tesoreria",
]

_PROG_HEADER_RE = re.compile(r"^\s*(\d{10})\s+(\d+)\s+(.+)$")
_META_ITEM_RE = re.compile(r"^\s*(\d+)\b")
_UNIT_RE = re.compile(r"\(([^)]+)\)\s*$")


_NUM_TOKEN_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2})|-?\d{1,3}(?:\.\d{3})+|\d+")


def _parse_numeric_tokens(text: str) -> List[float]:
    vals = []
    for m in _NUM_TOKEN_RE.findall(text or ""):
        if "," in m:
            vals.append(parse_amount_ar(m))
        else:
            vals.append(float(m.replace(".", "")))
    return vals


def parse_metas_from_text(
    text_all: str, warnings: Optional[List[str]] = None
) -> List[Dict[str, object]]:
    if warnings is None:
        warnings = []

    rows: List[Dict[str, object]] = []
    current_juri: Optional[str] = None
    current_prog_code: Optional[str] = None
    current_prog_name: Optional[str] = None
    pending_meta: Optional[Dict[str, object]] = None
    in_section = False

    for raw_line in (text_all or "").splitlines():
        line = normalize_name(raw_line)
        if not line:
            continue

        low = normalize_key(line)
        if not in_section:
            if _SECTION_START in low:
                in_section = True
            continue

        if any(k in low for k in _SECTION_END_KEYWORDS):
            break

        if "programado" in low and "diferencia" in low:
            continue
        if low in ("departamento ejecutivo", "h.c.d."):
            continue

        prog_match = _PROG_HEADER_RE.match(line)
        if prog_match:
            current_juri = prog_match.group(1)
            current_prog_code = prog_match.group(2)
            current_prog_name = normalize_name(prog_match.group(3))
            pending_meta = None
            continue

        if current_juri is None or current_prog_code is None:
            continue

        nums = _parse_numeric_tokens(line)
        meta_match = _META_ITEM_RE.match(line)
        is_meta_line = bool(meta_match)

        if is_meta_line:
            meta_text = _META_ITEM_RE.sub("", line).strip()
            unit = None
            unit_match = _UNIT_RE.search(meta_text)
            if unit_match:
                unit = normalize_name(unit_match.group(1))
                meta_text = normalize_name(meta_text[: unit_match.start()])

            pending_meta = {
                "Juri_Codigo": current_juri,
                "Prog_Codigo": current_prog_code,
                "Prog_Nombre": current_prog_name,
                "Meta_Nombre": meta_text,
                "Meta_Unidad": unit,
                "Meta_Anual": None,
                "Meta_Parcial": None,
                "Meta_Ejecutado": None,
                "Meta_Observacion": None,
            }

            if len(nums) >= 3:
                pending_meta["Meta_Anual"] = nums[-4] if len(nums) >= 4 else nums[-3]
                pending_meta["Meta_Parcial"] = nums[-3]
                pending_meta["Meta_Ejecutado"] = nums[-2]
                rows.append(pending_meta)
                pending_meta = None
            continue

        if pending_meta and len(nums) >= 3:
            pending_meta["Meta_Anual"] = nums[-4] if len(nums) >= 4 else nums[-3]
            pending_meta["Meta_Parcial"] = nums[-3]
            pending_meta["Meta_Ejecutado"] = nums[-2]
            rows.append(pending_meta)
            pending_meta = None
            continue

    if not rows:
        warnings.append("No se encontraron metas.")

    return rows
