from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

from openai import OpenAI

from ingest.extract_juri_prog import extract_jurisdicciones_programas
from ingest.extract_metas import extract_metas
from ingest.logs import log_event
from ingest.router import route_sections
from ingest.settings import load_settings
from ingest.supabase_io import (
    build_client,
    create_document,
    fetch_programas_mapping,
    insert_metas_staging,
    update_document_status,
    upsert_jurisdicciones,
    upsert_metas,
    upsert_programas,
)
from ingest.validate import validate_juri_prog, validate_metas


def _build_log_path() -> str:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join("logs", f"ingest_{timestamp}.jsonl")


def _require_settings(settings) -> None:
    missing = []
    if not settings.openai_api_key:
        missing.append("OPENAI_API_KEY")
    if not settings.supabase_url:
        missing.append("SUPABASE_URL")
    if not settings.supabase_key:
        missing.append("SUPABASE_KEY")
    if missing:
        raise RuntimeError(f"Variables faltantes: {', '.join(missing)}")


def _prep_juri_rows(doc_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("jurisdicciones", []):
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "Juri_Codigo": item.get("juri_codigo"),
                "Juri_Nombre": item.get("juri_nombre"),
                "Monto_Vigente": item.get("monto_vigente"),
                "Monto_Devengado": item.get("monto_devengado"),
                "Monto_Pagado": item.get("monto_pagado"),
            }
        )
    return rows


def _prep_prog_rows(doc_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("programas", []):
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "Juri_Codigo": item.get("juri_codigo"),
                "Prog_Codigo": item.get("prog_codigo"),
                "Prog_Nombre": item.get("prog_nombre"),
                "Monto_Vigente": item.get("monto_vigente"),
                "Monto_Devengado": item.get("monto_devengado"),
                "Monto_Pagado": item.get("monto_pagado"),
            }
        )
    return rows


def _prep_metas_rows(doc_id: str, metas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for meta in metas:
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Programa": meta.get("id_programa"),
                "Meta_Codigo": meta.get("meta_codigo"),
                "Meta_Nombre": meta.get("meta_nombre"),
                "Unidad_Medida": meta.get("unidad_medida"),
                "Meta_Valores": meta.get("valores"),
            }
        )
    return rows


def _upsert_metas_by_key(client, metas_rows: list[dict[str, Any]]) -> None:
    with_codigo = [row for row in metas_rows if row.get("Meta_Codigo")]
    sin_codigo = [row for row in metas_rows if not row.get("Meta_Codigo")]
    if with_codigo:
        upsert_metas(
            client,
            with_codigo,
            on_conflict="ID_DocumentoCargado,ID_Programa,Meta_Codigo",
        )
    if sin_codigo:
        upsert_metas(
            client,
            sin_codigo,
            on_conflict="ID_DocumentoCargado,ID_Programa,Meta_Nombre",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="LLM-assisted ETL para PDFs municipales.")
    parser.add_argument("--pdf", required=True, help="Ruta al PDF local")
    parser.add_argument("--municipio", required=True, help="Nombre del municipio")
    parser.add_argument("--periodo", required=True, help="Periodo (ej: Q1 2025)")
    parser.add_argument("--tipo", required=True, help="Tipo de documento (ej: Rendicion)")
    args = parser.parse_args()

    settings = load_settings()
    _require_settings(settings)
    log_path = _build_log_path()
    log_event(log_path, "ingest_start", {"pdf": args.pdf})

    client_openai = OpenAI(api_key=settings.openai_api_key)
    client_supabase = build_client(settings.supabase_url, settings.supabase_key)

    doc_id = create_document(client_supabase, args.municipio, args.periodo, args.tipo)
    log_event(log_path, "doc_created", {"doc_id": doc_id})

    router = route_sections(args.pdf)
    log_event(
        log_path,
        "router_done",
        {
            "juri_prog_pages": router.juri_prog_pages,
            "metas_pages": router.metas_pages,
            "fallback": router.used_fallback,
        },
    )

    base_dir = os.path.dirname(__file__)
    juri_schema = os.path.join(base_dir, "schemas", "jurisdicciones_programas.schema.json")
    metas_schema = os.path.join(base_dir, "schemas", "metas.schema.json")

    juri_prog_payload = extract_jurisdicciones_programas(
        client=client_openai,
        schema_path=juri_schema,
        pdf_path=args.pdf,
        router=router,
        log_path=log_path,
        model=settings.openai_model,
        max_retries=settings.max_retries,
        retry_sleep_sec=settings.retry_sleep_sec,
    )

    juri_prog_warnings = validate_juri_prog(juri_prog_payload)
    if juri_prog_warnings:
        log_event(log_path, "juri_prog_warnings", {"warnings": juri_prog_warnings})

    juri_rows = _prep_juri_rows(doc_id, juri_prog_payload)
    prog_rows = _prep_prog_rows(doc_id, juri_prog_payload)
    upsert_jurisdicciones(client_supabase, juri_rows)
    upsert_programas(client_supabase, prog_rows)

    program_rows = fetch_programas_mapping(client_supabase, doc_id)
    metas_resueltas, metas_sin_programa = extract_metas(
        client=client_openai,
        schema_path=metas_schema,
        pdf_path=args.pdf,
        router=router,
        program_rows=program_rows,
        log_path=log_path,
        model=settings.openai_model,
        max_retries=settings.max_retries,
        retry_sleep_sec=settings.retry_sleep_sec,
    )

    if metas_sin_programa:
        log_event(log_path, "metas_sin_programa", {"rows": metas_sin_programa})
        if settings.metas_staging_table:
            insert_metas_staging(client_supabase, settings.metas_staging_table, metas_sin_programa)

    metas_rows = _prep_metas_rows(doc_id, metas_resueltas)
    _upsert_metas_by_key(client_supabase, metas_rows)

    metas_warnings = validate_metas(metas_rows)
    if metas_warnings:
        log_event(log_path, "metas_warnings", {"warnings": metas_warnings})

    summary = {
        "doc_id": doc_id,
        "jurisdicciones": len(juri_rows),
        "programas": len(prog_rows),
        "metas": len(metas_rows),
        "metas_sin_programa": len(metas_sin_programa),
        "warnings": juri_prog_warnings + metas_warnings,
    }
    update_document_status(client_supabase, doc_id, "completado", summary)
    log_event(log_path, "ingest_done", summary)

    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
