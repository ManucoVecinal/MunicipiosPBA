# -*- coding: utf-8 -*-
"""
Parser de cuentas desde la seccion Demostracion del Saldo.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

from .common import normalize_key, normalize_name, parse_amount_ar, find_amounts


_SECTION_START = "demostracion del saldo"
_SECTION_END_KEYWORDS = [
    "evolucion de las principales metas",
]

_CODE_RE = re.compile(r"^\d{6,12}\b")


def _tipo_from_nombre(name: str) -> str:
    low = normalize_key(name)
    if "banco" in low:
        return "BANCO"
    if "fondos" in low:
        return "FONDOS"
    if "recursos" in low or "afectados" in low:
        return "RECURSOS"
    if "caja" in low:
        return "CAJA"
    return "OTRO"


def parse_cuentas_from_text(
    text_all: str, warnings: Optional[List[str]] = None
) -> List[Dict[str, object]]:
    if warnings is None:
        warnings = []

    rows: List[Dict[str, object]] = []
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

        if "total" in low:
            continue

        # Evitar lineas mezcladas con situacion patrimonial
        if "pasivo" in low or "activo" in low or "patrimonio" in low:
            continue

        codes = re.findall(r"\b\d{9}\b", line)
        if len(codes) > 1:
            continue

        amounts = find_amounts(line)
        if not amounts:
            continue

        importe = parse_amount_ar(amounts[-1])
        line_wo_amount = line[: line.rfind(amounts[-1])].strip()
        if not line_wo_amount:
            continue

        if line_wo_amount.lower().startswith("caja"):
            rows.append(
                {
                    "Cuenta_Codigo": None,
                    "Cuenta_Nombre": "CAJA",
                    "Cuenta_Tipo": "CAJA",
                    "Cuenta_Importe": importe,
                }
            )
            continue

        code_match = _CODE_RE.match(line_wo_amount)
        if code_match:
            code = code_match.group(0)
            nombre = line_wo_amount[len(code) :].strip()
            if not nombre:
                nombre = code
            rows.append(
                {
                    "Cuenta_Codigo": code,
                    "Cuenta_Nombre": nombre,
                    "Cuenta_Tipo": _tipo_from_nombre(nombre),
                    "Cuenta_Importe": importe,
                }
            )
            continue

        rows.append(
            {
                "Cuenta_Codigo": None,
                "Cuenta_Nombre": line_wo_amount,
                "Cuenta_Tipo": _tipo_from_nombre(line_wo_amount),
                "Cuenta_Importe": importe,
            }
        )

    if not rows:
        warnings.append("No se encontraron cuentas.")

    return rows
