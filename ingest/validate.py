from __future__ import annotations

from typing import Any


def validate_juri_prog(payload: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    jurisdicciones = payload.get("jurisdicciones", [])
    programas = payload.get("programas", [])
    if not jurisdicciones:
        warnings.append("No se detectaron jurisdicciones.")
    if not programas:
        warnings.append("No se detectaron programas.")

    juri_codes = {item.get("juri_codigo") for item in jurisdicciones if item.get("juri_codigo")}
    for prog in programas:
        if not prog.get("juri_codigo"):
            warnings.append(f"Programa sin juri_codigo: {prog.get('prog_codigo') or prog.get('prog_nombre')}")
        elif prog.get("juri_codigo") not in juri_codes:
            warnings.append(
                f"Programa con juri_codigo desconocido: {prog.get('prog_codigo')} -> {prog.get('juri_codigo')}"
            )
    return warnings


def validate_metas(metas: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for meta in metas:
        id_programa = meta.get("id_programa") or meta.get("ID_Programa")
        if not id_programa:
            warnings.append(
                f"Meta sin id_programa: {meta.get('meta_codigo') or meta.get('meta_nombre') or meta.get('Meta_Codigo') or meta.get('Meta_Nombre')}"
            )
    return warnings
