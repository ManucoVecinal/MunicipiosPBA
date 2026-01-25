from __future__ import annotations

from typing import Any

from jsonschema import validate as json_validate


def validate_payload(payload: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    json_validate(instance=payload, schema=schema)
    if not payload.get("bd_jurisdiccion"):
        warnings.append("Sin jurisdicciones.")
    if not payload.get("bd_programas"):
        warnings.append("Sin programas.")
    return warnings
