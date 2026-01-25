# -*- coding: utf-8 -*-
"""
Parser de jurisdicciones y programas desde texto completo.
"""
from __future__ import annotations

import re
import unicodedata
from typing import List, Dict, Optional

from .recursos import parse_amount_ar, normalize_name


_AMOUNT_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2})")
_CODE_LINE_RE = re.compile(r"^(\d{4,})\s*-?\s*(\d{1,3})\s+(.+)$")
_SECTION_START = "evolucion de gastos por programa"
_SECTION_END_KEYWORDS = [
    "evolucion de los recursos",
    "evolucion de gastos por objeto",
    "evolucion de las principales metas",
    "movimientos de tesoreria",
    "estado de situacion patrimonial",
    "situacion economico-financiera",
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


def parse_programas_from_text(
    text_all: str, warnings: Optional[List[str]] = None
) -> Dict[str, List[Dict[str, object]]]:
    """
    Parsea jurisdicciones y programas desde texto completo.
    """
    if warnings is None:
        warnings = []

    jurisdicciones: List[Dict[str, object]] = []
    programas: List[Dict[str, object]] = []
    juri_actual = None
    current_group = None
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

        if _is_total_row(line):
            continue

        # Detectar jurisdiccion (linea sin codigo al inicio)
        code_match = _CODE_LINE_RE.match(line)
        if not code_match:
            amounts = _AMOUNT_RE.findall(line)
            name = normalize_name(_AMOUNT_RE.sub("", line))
            # Encabezados de jurisdiccion (sin importes)
            if len(amounts) == 0 and name in ["Departamento Ejecutivo", "H.C.D."]:
                current_group = name
                juri_actual = name
                jurisdicciones.append({"Juri_Nombre": name})
                continue

            # Si la linea tiene importes, la tratamos como jurisdiccion y programa
            if len(amounts) >= 5:
                juri_name = name
                prog_name = name

                if current_group == "H.C.D." and name.lower() == "actividades centrales":
                    juri_name = "HCD"
                    prog_name = "HCD"

                juri_actual = juri_name
                jurisdicciones.append({"Juri_Nombre": juri_name})

                vig = parse_amount_ar(amounts[-5])
                prev = parse_amount_ar(amounts[-4])
                comp = parse_amount_ar(amounts[-3])
                dev = parse_amount_ar(amounts[-2])
                pag = parse_amount_ar(amounts[-1])

                if not _is_total_row(prog_name):
                    programas.append(
                        {
                            "Juri_Nombre": juri_actual,
                            "Prog_Codigo": None,
                            "Prog_Nombre": prog_name,
                            "Prog_Vigente": vig,
                            "Prog_Preventivo": prev,
                            "Prog_Compromiso": comp,
                            "Prog_Devengado": dev,
                            "Prog_Pagado": pag,
                        }
                    )
            continue

        # Programas con codigo de jurisdiccion + codigo de programa
        juri_code = code_match.group(1)
        prog_code = code_match.group(2)
        prog_rest = code_match.group(3)
        amounts = _AMOUNT_RE.findall(prog_rest)
        if len(amounts) < 5:
            warnings.append(f"Fila de programa incompleta: '{line}'")
            continue

        vig = parse_amount_ar(amounts[-5])
        prev = parse_amount_ar(amounts[-4])
        comp = parse_amount_ar(amounts[-3])
        dev = parse_amount_ar(amounts[-2])
        pag = parse_amount_ar(amounts[-1])

        name = normalize_name(_AMOUNT_RE.sub("", prog_rest))
        if _is_total_row(name):
            continue

        # Cargar jurisdiccion por codigo (una sola por codigo)
        jurisdicciones.append(
            {
                "Juri_Codigo": juri_code,
                "Juri_Nombre": current_group or juri_actual,
            }
        )

        programas.append(
            {
                "Juri_Codigo": juri_code,
                "Prog_Codigo": prog_code,
                "Prog_Nombre": name,
                "Prog_Vigente": vig,
                "Prog_Preventivo": prev,
                "Prog_Compromiso": comp,
                "Prog_Devengado": dev,
                "Prog_Pagado": pag,
            }
        )

    return {"jurisdicciones": jurisdicciones, "programas": programas}
