# -*- coding: utf-8 -*-
"""
Parser de gastos por objeto desde texto completo.
"""
from __future__ import annotations

import re
import unicodedata
from typing import List, Dict, Optional

from .recursos import parse_amount_ar, normalize_name


_AMOUNT_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2})")
_SECTION_START = "evolucion de gastos por objeto"
_SECTION_END_KEYWORDS = [
    "evolucion de gastos por programa",
    "evolucion de los recursos",
]


def _normalize_key(text: str) -> str:
    text = normalize_name(text)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    return text.lower()


def _is_total_row(name: str) -> bool:
    n = normalize_name(name).lower()
    if n.startswith("total"):
        return True
    if "total general" in n:
        return True
    return False


def parse_gastos_objeto_from_text(
    text_all: str, warnings: Optional[List[str]] = None
) -> List[Dict[str, object]]:
    """
    Parsea gastos por objeto desde texto completo.
    """
    if warnings is None:
        warnings = []

    rows: List[Dict[str, object]] = []
    tipo_actual = None
    in_section = False

    for raw_line in text_all.splitlines():
        line = normalize_name(raw_line)
        if not line:
            continue

        low = _normalize_key(line)

        if not in_section:
            if _SECTION_START in low:
                in_section = True
            continue

        if any(k in low for k in _SECTION_END_KEYWORDS):
            break

        if low.startswith("1.") and "presupuest" in low:
            tipo_actual = "Presupuestarios"
            continue
        if low.startswith("2.") and "extrapresupuest" in low:
            tipo_actual = "Extrapresupuestarios"
            continue

        amounts = _AMOUNT_RE.findall(line)
        if len(amounts) >= 5:
            vig = parse_amount_ar(amounts[-5])
            prev = parse_amount_ar(amounts[-4])
            comp = parse_amount_ar(amounts[-3])
            dev = parse_amount_ar(amounts[-2])
            pag = parse_amount_ar(amounts[-1])

            name = normalize_name(_AMOUNT_RE.sub("", line))
            if _is_total_row(name):
                continue

            if not tipo_actual:
                warnings.append(f"Fila sin tipo detectado: '{line}'")
                continue

            rows.append(
                {
                    "Gasto_Categoria": tipo_actual,
                    "Gasto_Objeto": name,
                    "Gasto_Vigente": vig,
                    "Gasto_Preventivo": prev,
                    "Gasto_Compromiso": comp,
                    "Gasto_Devengado": dev,
                    "Gasto_Pagado": pag,
                }
            )

    return rows
