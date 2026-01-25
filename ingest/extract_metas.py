from __future__ import annotations

import os
from typing import Any

from openai import OpenAI

from ingest.llm_utils import call_structured_output, load_schema
from ingest.logs import log_event
from ingest.router import RouterResult


SYSTEM_PROMPT = """Eres un extractor de tablas de metas para PDFs municipales.
Devuelves SOLO JSON valido siguiendo el JSON Schema provisto.
No inventes datos. Ignora encabezados, totales y filas decorativas."""


USER_PROMPT_TEMPLATE = """Extrae la tabla "Evolucion de las principales metas de programas".
Reglas:
- Cada fila representa una meta vinculada a un programa.
- Captura codigo/nombre de meta, unidad de medida y valores del periodo.
- Captura el codigo y nombre del programa (y el codigo de jurisdiccion si aparece).
- Ignora encabezados, subtitulos, totales, separadores y filas vacias.
- Normaliza numeros: "1.234,56" => 1234.56 y "(123,45)" => -123.45.

{input_section}
"""


def _build_input_section(router: RouterResult) -> str:
    if router.metas_pages and router.page_texts:
        chunks = []
        for page in router.metas_pages:
            text = router.page_texts.get(page, "")
            if text.strip():
                chunks.append(f"[PAGINA {page}]\n{text}")
        if chunks:
            return "Texto extraido por paginas:\n" + "\n\n".join(chunks)
    if router.page_texts:
        chunks = []
        for page, text in router.page_texts.items():
            if text.strip():
                chunks.append(f"[PAGINA {page}]\n{text}")
        if chunks:
            return "Texto extraido del documento completo:\n" + "\n\n".join(chunks)
    return "Usa el PDF completo adjunto como input."


def _norm(text: str) -> str:
    return " ".join(text.strip().lower().split())


def _build_program_mapping(rows: list[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in rows:
        prog_codigo = (row.get("Prog_Codigo") or "").strip()
        juri_codigo = (row.get("Juri_Codigo") or "").strip()
        prog_nombre = (row.get("Prog_Nombre") or "").strip()
        id_programa = row.get("ID_Programa")
        if not id_programa:
            continue
        if prog_codigo:
            mapping[prog_codigo] = id_programa
        if juri_codigo and prog_codigo:
            mapping[f"{juri_codigo}::{prog_codigo}"] = id_programa
        if prog_nombre:
            mapping[f"nombre::{_norm(prog_nombre)}"] = id_programa
        if juri_codigo and prog_nombre:
            mapping[f"{juri_codigo}::nombre::{_norm(prog_nombre)}"] = id_programa
    return mapping


def _resolve_program_id(
    mapping: dict[str, str], meta: dict[str, Any]
) -> str | None:
    prog_codigo = (meta.get("prog_codigo") or "").strip()
    juri_codigo = (meta.get("juri_codigo") or "").strip()
    prog_nombre = (meta.get("prog_nombre") or "").strip()
    if juri_codigo and prog_codigo:
        return mapping.get(f"{juri_codigo}::{prog_codigo}") or mapping.get(prog_codigo)
    if prog_codigo:
        return mapping.get(prog_codigo)
    if juri_codigo and prog_nombre:
        return mapping.get(f"{juri_codigo}::nombre::{_norm(prog_nombre)}") or mapping.get(
            f"nombre::{_norm(prog_nombre)}"
        )
    if prog_nombre:
        return mapping.get(f"nombre::{_norm(prog_nombre)}")
    return None


def extract_metas(
    client: OpenAI,
    schema_path: str,
    pdf_path: str,
    router: RouterResult,
    program_rows: list[dict[str, Any]],
    log_path: str,
    model: str,
    max_retries: int,
    retry_sleep_sec: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    schema = load_schema(schema_path)
    input_section = _build_input_section(router)
    user_prompt = USER_PROMPT_TEMPLATE.format(input_section=input_section)
    input_file_id = None
    used_fallback = "PDF completo adjunto" in input_section
    if used_fallback:
        with open(pdf_path, "rb") as handle:
            upload = client.files.create(file=handle, purpose="assistants")
        input_file_id = upload.id

    log_event(
        log_path,
        "extract_metas_start",
        {
            "pdf_path": os.path.abspath(pdf_path),
            "pages": router.metas_pages,
            "fallback": used_fallback,
        },
    )
    payload = call_structured_output(
        client=client,
        model=model,
        schema=schema,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        input_file_id=input_file_id,
        max_retries=max_retries,
        retry_sleep_sec=retry_sleep_sec,
    )
    metas = payload.get("metas", [])
    mapping = _build_program_mapping(program_rows)
    resolved: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for meta in metas:
        id_programa = _resolve_program_id(mapping, meta)
        if not id_programa:
            unresolved.append(meta)
            continue
        meta["id_programa"] = id_programa
        resolved.append(meta)

    log_event(
        log_path,
        "extract_metas_done",
        {
            "metas_total": len(metas),
            "metas_resueltas": len(resolved),
            "metas_sin_programa": len(unresolved),
        },
    )
    return resolved, unresolved
