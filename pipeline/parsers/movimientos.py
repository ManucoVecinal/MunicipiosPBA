# -*- coding: utf-8 -*-
"""
Parser de Movimientos de Tesoreria.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

from .common import normalize_key, normalize_name, parse_amount_ar, find_amounts


_SECTION_START = "movimientos de tesoreria"
_SECTION_END_KEYWORDS = [
    "estado de situacion patrimonial",
    "demostracion del saldo",
    "evolucion de las principales metas",
]

_PERIODO_RE = re.compile(r"Del\s+\d{2}/\d{2}/\d{4}\s+al\s+\d{2}/\d{2}/\d{4}")

_MOV_TYPES = [
    ("Saldo Inicial", "saldo inicial"),
    ("Ingreso", "ingresos del periodo"),
    ("Ingreso", "ingresos de ajustes contables"),
    ("Egreso", "gastos del periodo"),
    ("Egreso", "egresos de ajustes contables"),
    # La constraint solo permite Saldo Inicial/Ingreso/Egreso.
    ("Saldo Inicial", "saldo final"),
]


def extract_periodo(text_all: str) -> Optional[str]:
    match = _PERIODO_RE.search(text_all or "")
    return match.group(0) if match else None


def parse_movimientos_from_text(
    text_all: str, warnings: Optional[List[str]] = None
) -> List[Dict[str, object]]:
    if warnings is None:
        warnings = []

    rows: List[Dict[str, object]] = []
    in_section = False
    found = {}

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

        if "movimientos de tesoreria" in low and "importe" in low:
            continue

        # Cortar columna derecha (estado patrimonial) si aparece un codigo 9 digitos
        code_idx = re.search(r"\b\d{9}\b", line)
        if code_idx:
            line = line[: code_idx.start()].strip()
            low = normalize_key(line)
            if not line:
                continue

        amounts = find_amounts(line)
        if not amounts:
            continue

        for slug, label in _MOV_TYPES:
            if label in low and slug not in found:
                importe = parse_amount_ar(amounts[0])
                tipo_text = line
                if ":" in line:
                    tipo_text = line.split(":", 1)[0]
                tipo_text = normalize_name(tipo_text)
                found[slug] = {
                    "MovTes_Tipo": tipo_text,
                    "MovTes_Importe": importe,
                    "MovTes_TipoResumido": slug,
                }
                break

        if len(found) == len(_MOV_TYPES):
            break

    for slug, _label in _MOV_TYPES:
        if slug in found:
            rows.append(found[slug])

    if not rows:
        warnings.append("No se encontraron movimientos de tesoreria.")

    return rows
