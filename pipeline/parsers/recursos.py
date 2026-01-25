# -*- coding: utf-8 -*-
"""
Parser de recursos (SIT-ECO) desde texto completo.
"""
from __future__ import annotations

import re
import unicodedata
from typing import List, Dict, Optional


_AMOUNT_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2})")
_SECTION_START = "evolucion de los recursos"
_SECTION_END_KEYWORDS = [
    "evolucion de los gastos",
    "evolucion del gasto",
    "estado de ejecucion del gasto",
    "estado de ejecucion de gastos",
    "evolucion de gastos por objeto",
    "evolucion de gastos por programa",
    "evolucion de las principales metas",
    "movimientos de tesoreria",
    "estado de situacion patrimonial",
    "cuenta ahorro inversion financiamiento",
]
_RIGHT_COL_RE = re.compile(
    r"(cuenta\s+ahorro|ahorro\s+corriente|gastos\s+corrientes|gastos\s+de\s+capital|"
    r"resultado\s+financiero|ingresos\s+totales|gastos\s+totales|fuentes\s+financieras|"
    r"aplicaciones\s+financieras)",
    re.IGNORECASE,
)
_ROMAN_RIGHT_RE = re.compile(r"^\s*[ivx]+\.", re.IGNORECASE)
_ROMAN_INLINE_RE = re.compile(r"\b[IVX]+\.")


def _normalize_key(text: str) -> str:
    text = normalize_name(text)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    return text.lower()


def parse_amount_ar(value: str) -> float:
    """
    Parsea numeros con formato AR (punto miles, coma decimal).
    """
    if value is None:
        raise ValueError("Valor vacio.")
    s = value.strip()
    if not s:
        raise ValueError("Valor vacio.")

    # Quitar separador de miles y convertir coma decimal a punto
    s = s.replace(".", "").replace(",", ".")
    return float(s)


def normalize_name(text: str) -> str:
    """
    Normaliza nombre: trim + colapsar espacios.
    """
    if text is None:
        return ""
    return " ".join(text.strip().split())


def _is_total_row(name: str) -> bool:
    n = normalize_name(name).lower()
    if n.startswith("total"):
        return True
    if "total general" in n:
        return True
    return False


def _name_from_tipo_line(line: str, tipo: str) -> str:
    clean = normalize_name(line)
    clean = re.sub(r"^\s*\d+\.\s*", "", clean)
    if tipo.lower().startswith("presup"):
        clean = re.sub(r"\bpresupuestarios?\b", "", clean, flags=re.IGNORECASE)
    if tipo.lower().startswith("extra"):
        clean = re.sub(r"\bextrapresupuestarios?\b", "", clean, flags=re.IGNORECASE)
    clean = normalize_name(clean)
    return clean or tipo


def _split_left_column(line: str) -> str:
    roman = _ROMAN_INLINE_RE.search(line)
    if roman:
        return line[: roman.start()].strip()
    match = _RIGHT_COL_RE.search(line)
    if match:
        return line[: match.start()].strip()
    return line


def _is_right_column_line(line: str) -> bool:
    if _ROMAN_RIGHT_RE.match(line):
        return True
    low = _normalize_key(line)
    if "cuenta ahorro inversion financiamiento" in low:
        return True
    if "resultado financiero" in low:
        return True
    return False


def parse_recursos_from_text(
    text_all: str, warnings: Optional[List[str]] = None
) -> List[Dict[str, object]]:
    """
    Parsea recursos desde texto completo.

    Usa una maquina de estados para tipo/subtipo.
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
            if _SECTION_START in low or ("evolucion" in low and "recursos" in low):
                in_section = True
            continue

        if any(k in low for k in _SECTION_END_KEYWORDS):
            break

        if _is_right_column_line(line):
            continue

        line = _split_left_column(line)
        if not line:
            continue

        low = _normalize_key(line)

        # Detectar cambios de tipo
        tipo_line = False
        if low.startswith("1.") and "presupuest" in low:
            tipo_actual = "Presupuestario"
            tipo_line = True
        if low.startswith("2.") and "extrapresupuest" in low:
            tipo_actual = "Extrapresupuestario"
            tipo_line = True

        # Extraer importes
        amounts = _AMOUNT_RE.findall(line)
        if len(amounts) >= 3:
            vig = parse_amount_ar(amounts[0])
            dev = parse_amount_ar(amounts[1])
            per = parse_amount_ar(amounts[2])

            first_amount = amounts[0]
            idx = line.find(first_amount)
            name = normalize_name(line[:idx]) if idx > 0 else normalize_name(line)
            if _is_total_row(name):
                continue

            if not tipo_actual:
                warnings.append(f"Fila sin tipo detectado: '{line}'")
                continue

            rows.append(
                {
                    "Rec_Tipo": tipo_actual,
                    "Rec_Nombre": name,
                    "Rec_Categoria": name,
                    "Rec_Vigente": vig,
                    "Rec_Devengado": dev,
                    "Rec_Percibido": per,
                }
            )
        elif len(amounts) == 1 and tipo_line and tipo_actual == "Presupuestario":
            # Encabezado mezclado con columna derecha, ignorar
            continue
        elif len(amounts) == 1 and tipo_actual == "Extrapresupuestario":
            per = parse_amount_ar(amounts[0])
            if tipo_line:
                name = tipo_actual
            else:
                idx = line.find(amounts[0])
                name = normalize_name(line[:idx]) if idx > 0 else normalize_name(line)
            if _is_total_row(name):
                continue
            rows.append(
                {
                    "Rec_Tipo": tipo_actual,
                    "Rec_Nombre": name,
                    "Rec_Categoria": name,
                    "Rec_Vigente": None,
                    "Rec_Devengado": None,
                    "Rec_Percibido": per,
                }
            )
        elif len(amounts) > 0 and len(amounts) != 3:
            warnings.append(f"Fila con cantidad de importes inesperada: '{line}'")

    return rows
