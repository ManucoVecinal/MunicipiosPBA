from __future__ import annotations

import json
import time
from typing import Any

from jsonschema import validate as json_validate
from openai import OpenAI


def normalize_number(value: str) -> float | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    cleaned = cleaned.strip("()")
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return -number if negative else number


def load_schema(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def call_structured_output(
    client: OpenAI,
    model: str,
    schema: dict[str, Any],
    system_prompt: str,
    user_prompt: str,
    input_file_id: str | None = None,
    max_retries: int = 2,
    retry_sleep_sec: float = 2.5,
) -> dict[str, Any]:
    use_responses = hasattr(client, "responses")
    schema_payload = schema.get("schema", schema)
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            if use_responses:
                try:
                    content = []
                    if input_file_id:
                        content.append({"type": "input_file", "file_id": input_file_id})
                    content.append({"type": "text", "text": user_prompt})
                    response = client.responses.create(
                        model=model,
                        input=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": content},
                        ],
                        response_format={"type": "json_schema", "json_schema": schema},
                    )
                    text = getattr(response, "output_text", None)
                    if text is None:
                        text = response.output[0].content[0].text  # type: ignore[index]
                    return json.loads(text)
                except TypeError as exc:
                    if "response_format" in str(exc):
                        use_responses = False
                    else:
                        raise

            schema_text = json.dumps(schema_payload, ensure_ascii=True)
            fallback_user = (
                f"{user_prompt}\n\n"
                "Devuelve SOLO JSON valido y estricto segun este JSON Schema:\n"
                f"{schema_text}"
            )
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": fallback_user},
                ],
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content
            if not text:
                raise RuntimeError("Respuesta vacia del LLM.")
            parsed = json.loads(text)
            json_validate(instance=parsed, schema=schema_payload)
            return parsed
        except Exception as exc:  # pragma: no cover - network/response errors
            last_error = exc
            if attempt < max_retries:
                time.sleep(retry_sleep_sec * (attempt + 1))
            else:
                raise RuntimeError(
                    f"LLM fallo despues de reintentos: {last_error}"
                ) from last_error
    raise RuntimeError("LLM fallo.") from last_error
