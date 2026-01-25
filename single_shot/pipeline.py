from __future__ import annotations

import json
import unicodedata
from typing import Any

from openai import OpenAI

from single_shot.logger import log_event
from single_shot.openai_extract import (
    build_schema,
    extract_metas_only,
    extract_pdf_single_shot,
)
from single_shot.supabase_io import (
    create_document,
    fetch_jurisdicciones,
    fetch_programas_for_juris,
    insert_metas_staging,
    update_document_status,
    upsert_cuentas,
    upsert_gastos,
    upsert_jurisdicciones,
    upsert_metas,
    upsert_movimientos,
    upsert_programas,
    upsert_recursos,
    upsert_sitpat,
)
from single_shot.validate import validate_payload


_SITPAT_NAMES = {
    "ACTIVO CORRIENTE": "Activo",
    "ACTIVO NO CORRIENTE": "Activo",
    "PASIVO CORRIENTE": "Pasivo",
    "PASIVO NO CORRIENTE": "Pasivo",
    "CAPITAL FISCAL": "Patrimonio Publico",
    "RESULTADO DE EJERCICIOS ANTERIORES": "Patrimonio Publico",
    "RESULTADO DEL EJERCICIO": "Patrimonio Publico",
    "RESULTADOS AFECTADOS A CONSTRUCCION DE BIENES DE DOMINIO PUBLICO": "Patrimonio Publico",
}


def _norm_text(value: str | None) -> str:
    if not value:
        return ""
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch)
    )
    return " ".join(text.strip().upper().split())


def _reclassify_cuentas_to_sitpat(payload: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    cuentas = payload.get("bd_cuentas", []) or []
    sitpat = payload.get("bd_situacionpatrimonial", []) or []

    kept_cuentas: list[dict[str, Any]] = []
    moved = 0
    for item in cuentas:
        name = _norm_text(item.get("Cuenta_Nombre"))
        code = (item.get("Cuenta_Codigo") or "").strip()
        is_numeric_code = code.isdigit() and len(code) >= 6
        sitpat_tipo = _SITPAT_NAMES.get(name)
        if sitpat_tipo and (is_numeric_code or name.startswith(("ACTIVO", "PASIVO", "CAPITAL", "RESULTADO"))):
            sitpat.append(
                {
                    "SitPat_Tipo": sitpat_tipo,
                    "SitPat_Nombre": item.get("Cuenta_Nombre"),
                    "SitPat_Saldo": item.get("Cuenta_Importe"),
                }
            )
            moved += 1
        else:
            kept_cuentas.append(item)

    if moved:
        warnings.append(f"Se movieron {moved} filas de bd_cuentas a bd_situacionpatrimonial.")

    payload["bd_cuentas"] = kept_cuentas
    payload["bd_situacionpatrimonial"] = sitpat
    return warnings


def _map_juri(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_jurisdiccion", []):
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "Juri_Codigo": item.get("Juri_Codigo"),
                "Juri_Nombre": item.get("Juri_Nombre"),
                "Juri_Descripcion": item.get("Juri_Grupo"),
            }
        )
    return rows


def _map_programas(
    payload: dict[str, Any],
    juri_map: dict[str, str],
) -> tuple[list[dict[str, Any]], list[str]]:
    rows = []
    warnings: list[str] = []
    for item in payload.get("bd_programas", []):
        juri_codigo = item.get("Juri_Codigo") or ""
        juri_id = juri_map.get(str(juri_codigo).strip())
        if not juri_id:
            warnings.append(f"Programa sin jurisdiccion: {item.get('Prog_Nombre')}")
            continue
        rows.append(
            {
                "ID_Jurisdiccion": juri_id,
                "Prog_Codigo": item.get("Prog_Codigo"),
                "Prog_Nombre": item.get("Prog_Nombre"),
                "Prog_Vigente": item.get("Prog_Vigente"),
                "Prog_Preventivo": item.get("Prog_Preventivo"),
                "Prog_Compromiso": item.get("Prog_Compromiso"),
                "Prog_Devengado": item.get("Prog_Devengado"),
                "Prog_Pagado": item.get("Prog_Pagado"),
            }
        )
    return rows, warnings


def _map_cuentas(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_cuentas", []):
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "Cuenta_Codigo": item.get("Cuenta_Codigo"),
                "Cuenta_Nombre": item.get("Cuenta_Nombre"),
                "Cuenta_Importe": item.get("Cuenta_Importe"),
            }
        )
    return rows


def _map_gastos(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_gastos", []):
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "Gasto_Categoria": item.get("Gasto_Categoria"),
                "Gasto_Objeto": item.get("Gasto_Objeto"),
                "Gasto_Vigente": item.get("Gasto_Vigente"),
                "Gasto_Preventivo": item.get("Gasto_Preventivo"),
                "Gasto_Compromiso": item.get("Gasto_Compromiso"),
                "Gasto_Devengado": item.get("Gasto_Devengado"),
                "Gasto_Pagado": item.get("Gasto_Pagado"),
            }
        )
    return rows


def _map_recursos(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_recursos", []):
        rec_categoria = item.get("Rec_Categoria")
        rec_tipo = item.get("Rec_TipoRecurso")
        rec_tipo_norm = None
        if rec_categoria:
            cat_norm = _norm_text(rec_categoria)
            if "PRESUPUEST" in cat_norm:
                rec_tipo_norm = "Presupuestario"
            if "EXTRAPRESUPUEST" in cat_norm:
                rec_tipo_norm = "Extrapresupuestario"
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "Rec_Categoria": rec_categoria,
                "Rec_Nombre": rec_tipo,
                "Rec_Tipo": rec_tipo_norm or rec_tipo,
                "Rec_Vigente": item.get("Rec_Vigente"),
                "Rec_Devengado": item.get("Rec_Devengado"),
                "Rec_Percibido": item.get("Rec_Percibido"),
            }
        )
    return rows


def _normalize_movtes_tipo(value: str | None) -> str | None:
    if not value:
        return None
    key = _norm_text(value).lower()
    if "saldo" in key and "final" in key:
        return None
    if "saldo" in key and "inicial" in key:
        return "Saldo Inicial"
    if "ingreso" in key:
        return "Ingreso"
    if "gasto" in key or "egreso" in key:
        return "Egreso"
    return None

def _map_movimientos(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_movimientosTesoreria", []):
        mov_tipo = item.get("MovTes_Tipo")
        mov_tipo_res = _normalize_movtes_tipo(mov_tipo)
        if not mov_tipo_res:
            continue
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "MovTes_Tipo": mov_tipo,
                "MovTes_TipoResumido": mov_tipo_res,
                "MovTes_Importe": item.get("MovTes_Importe"),
            }
        )
    return rows


def _map_sitpat(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_situacionpatrimonial", []):
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "SitPat_Tipo": item.get("SitPat_Tipo"),
                "SitPat_Nombre": item.get("SitPat_Nombre"),
                "SitPat_Saldo": item.get("SitPat_Saldo"),
            }
        )
    return rows


def _build_program_mapping(
    program_rows: list[dict[str, Any]],
    juri_map: dict[str, str],
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    juri_by_id = {v: k for k, v in juri_map.items() if v}
    for row in program_rows:
        prog_codigo = (row.get("Prog_Codigo") or "").strip()
        juri_id = row.get("ID_Jurisdiccion")
        juri_codigo = juri_by_id.get(juri_id, "")
        prog_id = row.get("ID_Programa")
        if not prog_id:
            continue
        if prog_codigo:
            mapping[prog_codigo] = prog_id
        if juri_codigo and prog_codigo:
            mapping[f"{juri_codigo}::{prog_codigo}"] = prog_id
    return mapping


def _create_programs_from_metas(
    payload: dict[str, Any],
    juri_map: dict[str, str],
    client_supabase,
) -> int:
    rows = []
    seen: set[tuple[str, str]] = set()
    for item in payload.get("bd_metas", []):
        juri_codigo = (item.get("Juri_Codigo") or "").strip()
        prog_codigo = (item.get("Prog_Codigo") or "").strip()
        prog_nombre = (item.get("Prog_Nombre") or "").strip()
        if not juri_codigo or not prog_codigo or not prog_nombre:
            continue
        key = (juri_codigo, prog_codigo)
        if key in seen:
            continue
        juri_id = juri_map.get(juri_codigo)
        if not juri_id:
            continue
        rows.append(
            {
                "ID_Jurisdiccion": juri_id,
                "Prog_Codigo": prog_codigo,
                "Prog_Nombre": prog_nombre,
            }
        )
        seen.add(key)
    if rows:
        upsert_programas(client_supabase, rows)
    return len(rows)


def _map_metas(
    payload: dict[str, Any],
    program_mapping: dict[str, str],
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    rows = []
    warnings: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for item in payload.get("bd_metas", []):
        juri_codigo = (item.get("Juri_Codigo") or "").strip()
        prog_codigo = (item.get("Prog_Codigo") or "").strip()
        key = f"{juri_codigo}::{prog_codigo}" if juri_codigo and prog_codigo else prog_codigo
        prog_id = program_mapping.get(key)
        if not prog_id:
            warnings.append(f"Meta sin programa: {item.get('Meta_Nombre')}")
            unresolved.append(item)
            continue
        rows.append(
            {
                "ID_Programa": prog_id,
                "Meta_Nombre": item.get("Meta_Nombre"),
                "Meta_Unidad": item.get("Meta_Unidad"),
                "Meta_Anual": item.get("Meta_Anual"),
                "Meta_Parcial": item.get("Meta_Parcial"),
                "Meta_Ejecutado": item.get("Meta_Ejecutado"),
                "Meta_Observacion": None,
            }
        )
    return rows, warnings, unresolved


def run_single_shot(
    *,
    client_openai: OpenAI,
    client_supabase,
    pdf_path: str,
    id_municipio: str,
    log_path: str,
    model: str,
    max_retries: int,
    retry_sleep_sec: float,
    metas_staging_table: str | None = None,
    doc_id: str | None = None,
    doc_nombre: str | None = None,
    doc_tipo: str | None = None,
    doc_periodo: str | None = None,
) -> dict[str, Any]:
    log_event(log_path, "single_shot_start", {"pdf": pdf_path, "doc_id": doc_id})

    if not doc_id:
        if not doc_nombre or not doc_tipo or not doc_periodo:
            raise RuntimeError("Faltan metadatos para crear documento.")
        doc_id = create_document(
            client_supabase,
            id_municipio=id_municipio,
            doc_nombre=doc_nombre,
            doc_tipo=doc_tipo,
            doc_periodo=doc_periodo,
        )
        log_event(log_path, "doc_created", {"doc_id": doc_id})

    payload = extract_pdf_single_shot(
        client=client_openai,
        model=model,
        pdf_path=pdf_path,
        max_retries=max_retries,
        retry_sleep_sec=retry_sleep_sec,
    )

    schema = build_schema()["schema"]
    warnings = validate_payload(payload, schema)
    metas_count = len(payload.get("bd_metas", []))
    if metas_count < 50:
        metas_only = extract_metas_only(
            client=client_openai,
            model=model,
            pdf_path=pdf_path,
            max_retries=max_retries,
            retry_sleep_sec=retry_sleep_sec,
        )
        metas_list = metas_only.get("bd_metas", [])
        if len(metas_list) > metas_count:
            payload["bd_metas"] = metas_list
            warnings.append("Metas recargadas en segunda pasada (metas only).")
    warnings.extend(_reclassify_cuentas_to_sitpat(payload))

    juri_rows = _map_juri(payload, doc_id, id_municipio)
    upsert_jurisdicciones(client_supabase, juri_rows)

    juri_db = fetch_jurisdicciones(client_supabase, doc_id)
    juri_map = {j.get("Juri_Codigo"): j.get("ID_Jurisdiccion") for j in juri_db if j.get("Juri_Codigo")}
    prog_rows, prog_warnings = _map_programas(payload, juri_map)
    warnings.extend(prog_warnings)
    upsert_programas(client_supabase, prog_rows)

    prog_db = fetch_programas_for_juris(client_supabase, list(juri_map.values()))
    program_mapping = _build_program_mapping(prog_db, juri_map)
    created = _create_programs_from_metas(payload, juri_map, client_supabase)
    if created:
        prog_db = fetch_programas_for_juris(client_supabase, list(juri_map.values()))
        program_mapping = _build_program_mapping(prog_db, juri_map)
    metas_rows, meta_warnings, metas_unresolved = _map_metas(payload, program_mapping)
    warnings.extend(meta_warnings)
    if metas_unresolved:
        log_event(log_path, "metas_unresolved", {"rows": metas_unresolved})
        if metas_staging_table:
            insert_metas_staging(client_supabase, metas_staging_table, metas_unresolved)
    upsert_metas(client_supabase, metas_rows)

    upsert_cuentas(client_supabase, _map_cuentas(payload, doc_id, id_municipio))
    upsert_gastos(client_supabase, _map_gastos(payload, doc_id, id_municipio))
    upsert_recursos(client_supabase, _map_recursos(payload, doc_id, id_municipio))
    upsert_movimientos(client_supabase, _map_movimientos(payload, doc_id, id_municipio))
    upsert_sitpat(client_supabase, _map_sitpat(payload, doc_id, id_municipio))

    summary = {
        "doc_id": doc_id,
        "counts": {
            "bd_cuentas": len(payload.get("bd_cuentas", [])),
            "bd_gastos": len(payload.get("bd_gastos", [])),
            "bd_recursos": len(payload.get("bd_recursos", [])),
            "bd_jurisdiccion": len(juri_rows),
            "bd_programas": len(prog_rows),
            "bd_metas": len(metas_rows),
            "bd_movimientosTesoreria": len(payload.get("bd_movimientosTesoreria", [])),
            "bd_situacionpatrimonial": len(payload.get("bd_situacionpatrimonial", [])),
        },
        "warnings": warnings + (payload.get("warnings") or []),
    }
    log_event(log_path, "single_shot_done", summary)
    update_document_status(client_supabase, doc_id, "completado", summary)
    return summary
