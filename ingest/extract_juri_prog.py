from __future__ import annotations

import os
from typing import Any

from openai import OpenAI

from ingest.llm_utils import call_structured_output, load_schema
from ingest.logs import log_event
from ingest.router import RouterResult


SYSTEM_PROMPT = """Eres un extractor de tablas para PDFs municipales.
Devuelves SOLO JSON valido siguiendo el JSON Schema provisto.
No inventes datos. Ignora encabezados, totales y filas decorativas."""


USER_PROMPT_TEMPLATE = """Extrae la tabla combinada de Jurisdicciones y Programas.
Reglas:
- La tabla mezcla niveles: filas de Jurisdiccion (nivel superior) y filas de Programa (subnivel).
- Cada programa debe quedar asociado a su jurisdiccion via juri_codigo.
- Para programas, captura los montos disponibles: vigente, preventivo, compromiso, devengado, pagado.
- Ignora encabezados, subtitulos, totales, separadores y filas vacias.
- Normaliza numeros: "1.234,56" => 1234.56 y "(123,45)" => -123.45.
- Mantener los codigos exactamente como aparecen en el PDF.

{input_section}
"""


def _build_input_section(router: RouterResult) -> str:
    if router.juri_prog_pages and router.page_texts:
        chunks = []
        for page in router.juri_prog_pages:
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


def extract_jurisdicciones_programas(
    client: OpenAI,
    schema_path: str,
    pdf_path: str,
    router: RouterResult,
    log_path: str,
    model: str,
    max_retries: int,
    retry_sleep_sec: float,
) -> dict[str, Any]:
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
        "extract_juri_prog_start",
        {
            "pdf_path": os.path.abspath(pdf_path),
            "pages": router.juri_prog_pages,
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
    log_event(
        log_path,
        "extract_juri_prog_done",
        {
            "jurisdicciones": len(payload.get("jurisdicciones", [])),
            "programas": len(payload.get("programas", [])),
        },
    )
    return payload
