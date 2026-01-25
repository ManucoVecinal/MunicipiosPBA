# -*- coding: utf-8 -*-
"""
Parser de Estado de Situacion Patrimonial.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

from .common import normalize_key, normalize_name, parse_amount_ar, find_amounts


_SECTION_START = "estado de situacion patrimonial"
_SECTION_END_KEYWORDS = [
    "demostracion del saldo",
    "evolucion de las principales metas",
    "movimientos de tesoreria",
]

_CODE_RE = re.compile(r"\b\d{9}\b")
_CODE_NAME_MAP = {
    "110000000": "Activo Corriente",
    "120000000": "Activo No Corriente",
    "210000000": "Pasivo Corriente",
    "220000000": "Pasivo No Corriente",
    "311000000": "Capital Fiscal",
    "312100000": "Resultados de Ejercicios Anteriores",
    "312200000": "Resultado del ejercicio",
    "312300000": "Resultados afectados a la construccion de bienes de dominio publico",
}


def _tipo_from_name(name: str, saldo: Optional[float]) -> str:
    low = normalize_key(name)
    if "activo" in low:
        return "ACTIVO"
    if "pasivo" in low or "patrimonio" in low:
        return "PASIVO_PATRIMONIO"
    if saldo is not None and saldo < 0:
        return "PASIVO_PATRIMONIO"
    return "PASIVO_PATRIMONIO"


def _pick_amount_after_code(line: str, code: str) -> Optional[float]:
    code_idx = line.find(code)
    if code_idx < 0:
        return None
    tail = line[code_idx + len(code) :]
    amounts = find_amounts(tail)
    if amounts:
        return parse_amount_ar(amounts[0])
    return None


def parse_sitpat_from_text(
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

        if "estado de situacion patrimonial" in low and "saldo" in low:
            continue
        if "movimientos de tesoreria" in low:
            continue

        for code, label in _CODE_NAME_MAP.items():
            if code in line and code not in found:
                saldo = _pick_amount_after_code(line, code)
                if saldo is None:
                    continue
                found[code] = {
                    "SitPat_Codigo": code,
                    "SitPat_Nombre": label,
                    "SitPat_Saldo": saldo,
                    "SitPat_Tipo": _tipo_from_name(label, saldo),
                }

        if len(found) == len(_CODE_NAME_MAP):
            break

    if found:
        for code in _CODE_NAME_MAP:
            if code in found:
                rows.append(found[code])
    if not rows:
        warnings.append("No se encontraron filas de situacion patrimonial.")

    return rows
