from __future__ import annotations

import argparse
import json
import os
import time

from openai import OpenAI

from single_shot.logger import log_event
from single_shot.pipeline import run_single_shot
from single_shot.settings import load_settings
from single_shot.supabase_io import build_client


def _build_log_path() -> str:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join("logs", f"single_shot_{timestamp}.jsonl")


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


def _map_programas(payload: dict[str, Any], juri_map: dict[str, str]) -> tuple[list[dict[str, Any]], list[str]]:
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
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "Rec_Categoria": item.get("Rec_Categoria"),
                "Rec_Tipo": item.get("Rec_TipoRecurso"),
                "Rec_Vigente": item.get("Rec_Vigente"),
                "Rec_Devengado": item.get("Rec_Devengado"),
                "Rec_Percibido": item.get("Rec_Percibido"),
            }
        )
    return rows


def _map_movimientos(payload: dict[str, Any], doc_id: str, id_municipio: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("bd_movimientosTesoreria", []):
        mov_tipo = item.get("MovTes_Tipo")
        rows.append(
            {
                "ID_DocumentoCargado": doc_id,
                "ID_Municipio": id_municipio,
                "MovTes_Tipo": mov_tipo,
                "MovTes_TipoResumido": mov_tipo,
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


def _build_program_mapping(program_rows: list[dict[str, Any]], juri_map: dict[str, str]) -> dict[str, str]:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-shot LLM ingest.")
    parser.add_argument("--pdf", required=True, help="Ruta al PDF local")
    parser.add_argument("--id_municipio", required=True, help="ID_Municipio")
    parser.add_argument("--doc_nombre", required=True, help="Nombre del documento")
    parser.add_argument("--doc_tipo", required=True, help="Tipo de documento")
    parser.add_argument("--periodo", required=True, help="Periodo (ej: Q1 2025)")
    args = parser.parse_args()

    settings = load_settings()
    _require_settings(settings)
    log_path = _build_log_path()
    log_event(log_path, "single_shot_start", {"pdf": args.pdf})

    client_openai = OpenAI(api_key=settings.openai_api_key)
    client_supabase = build_client(settings.supabase_url, settings.supabase_key)

    summary = run_single_shot(
        client_openai=client_openai,
        client_supabase=client_supabase,
        pdf_path=args.pdf,
        id_municipio=args.id_municipio,
        log_path=log_path,
        model=settings.openai_model,
        max_retries=settings.max_retries,
        retry_sleep_sec=settings.retry_sleep_sec,
        metas_staging_table=settings.metas_staging_table,
        doc_nombre=args.doc_nombre,
        doc_tipo=args.doc_tipo,
        doc_periodo=args.periodo,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
