# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from typing import Any

import pandas as pd
from jsonschema import validate as json_validate
from openai import OpenAI


SYSTEM_PROMPT = (
    "Sos un asistente de extraccion de datos contables desde XLSX tabulado. "
    "Tu tarea es convertir las tablas del Excel a JSON EXACTO segun el schema. "
    "No inventes filas. Si un dato no esta, dejalo vacio o null. "
    "Ignora encabezados, subtitulos y filas vacias. "
    "Normaliza numeros: '1.234,56' => 1234.56; '(123,45)' => -123.45. "
    "No devuelvas texto fuera del JSON."
)

USER_PROMPT = """Vas a recibir el contenido de un XLSX ya tabulado con 4 hojas fijas: "Table 1", "Table 2", "Table 3", "Table 4".
Necesito que cargues estas tablas (arrays) segun reglas:

Tabla 1 tiene dos secciones:
A) Evolucion de los Recursos -> bd_recursos
- Ignora encabezados: "Evolucion de los Recursos", "1. Presupuestarios", "2. Extrapresupuestarios", "Total", "Total General (1+2)".
- Por fila valida:
  Rec_Nombre = nombre de fila.
  Rec_Vigente, Rec_Devengado, Rec_Percibido = columnas numericas.
  Rec_Tipo = "Presupuestarios" si esta bajo 1; "Extrapresupuestarios" si esta bajo 2.
  Rec_Categoria = si la fila es "De libre disponibilidad" o "Afectados", guardar ese texto, sino null.
  Rec_Observacion = null.

B) Evolucion de Gastos por Objeto -> bd_gastos
- Ignora encabezados: "Evolucion de Gastos por Objeto", "1. Presupuestarios", "2. Extrapresupuestarios", "Total", "Total General (1+2)".
- Por fila valida:
  Gasto_Objeto = texto de la fila.
  Gasto_Vigente, Gasto_Preventivo, Gasto_Compromiso, Gasto_Devengado, Gasto_Pagado = columnas numericas.
  Gasto_Categoria = "Presupuestarios" o "Extrapresupuestarios" segun bloque.
  Gasto_Observacion = null.

Tabla 2: Evolucion de Gastos por Programa -> bd_programas y bd_jurisdiccion
- Ignora fila "Departamento Ejecutivo".
- Fila valida programa: ^(\\d{10})\\s*-?\\s*(\\d+)\\s+(.*)$
  Juri_Codigo = grupo 1 (10 digitos)
  Prog_Codigo = grupo 2
  Prog_Nombre = grupo 3
  Prog_Vigente, Prog_Preventivo, Prog_Compromiso, Prog_Devengado, Prog_Pagado desde columnas.
  Prog_Tipo = "Presupuestarios"
  Prog_Observacion = null
- Jurisdicciones:
  Juri_Codigo = codigo de 10 digitos.
  Juri_Nombre = si no aparece, usar "Jurisdiccion {codigo}".
  Juri_Descripcion, Juri_Orden, Juri_Observacion = null.

Tabla 3 tiene tres secciones:
A) Movimientos de Tesoreria -> bd_movimientosTesoreria
- En col A aparecen los tipos (Saldo Inicial, Ingresos del Periodo, Ingresos de Ajustes Contables, Gastos del periodo, Egresos de Ajustes Contables, Saldo Final).
- En col B estan los importes.
- Tomar las primeras 6 filas tipo+importe en ese orden.
- MovTes_Tipo = texto sin ":".
- MovTes_TipoResumido = igual a MovTes_Tipo.
- MovTes_Periodo = extraer del encabezado (ej: "Del 02/01/2025 al 30/09/2025").
- MovTes_Observacion = null.

B) Demostracion del Saldo -> bd_cuentas
- En col A hay un bloque multilinea con codigos de 9 digitos + nombre.
- En col B hay importes en el mismo orden.
- Parsear pares (Cuenta_Codigo, Cuenta_Nombre) con regex de 9 digitos.
- Asignar importes por indice, ignorando totales extra.
- Cuenta_Tipo = "CAJA" o null.

C) Estado de Situacion Patrimonial -> bd_situacionpatrimonial
- En col C hay codigos+nombre; en col D los saldos.
- Ignorar encabezados "Estado de Situacion Patrimonial", "ACTIVO", "PASIVO", "PATRIMONIO PUBLICO", "TOTAL ...".
- Item valido: ^(\\d[\\d\\.]*)\\s+(.*)$
- SitPat_Tipo = si el codigo empieza con "1" => "Activo"; si empieza con "2" => "Pasivo + Patrimonio Publico".
- SitPat_Saldo = saldo numerico.

Tabla 4: Evolucion de las principales metas de programas -> bd_metas
- Inicio de programa: regex (\\d{10})\\s+(\\d+)\\s+(.*)
  Juri_Codigo, Prog_Codigo, Prog_Nombre.
- Filas meta:
  Meta_Nombre = texto sin codigo ni unidad.
  Meta_Unidad = ultimo texto entre parentesis (si no hay, null).
  Meta_Anual, Meta_Parcial, Meta_Ejecutado en ese orden.
  Meta_Observacion puede guardar el codigo de meta si queres.
- Ignora columna "Diferencia".

Salida:
Devolve JSON con arrays:
bd_recursos, bd_gastos, bd_jurisdiccion, bd_programas, bd_metas,
bd_movimientosTesoreria, bd_cuentas, bd_situacionpatrimonial, warnings.
No devuelvas texto fuera del JSON.
"""


def build_schema() -> dict[str, Any]:
    return {
        "name": "xlsx_single_shot_ingest",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "bd_recursos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Rec_Nombre": {"type": "string"},
                            "Rec_Categoria": {"type": ["string", "null"]},
                            "Rec_Vigente": {"type": ["number", "null"]},
                            "Rec_Devengado": {"type": ["number", "null"]},
                            "Rec_Percibido": {"type": ["number", "null"]},
                            "Rec_Tipo": {"type": ["string", "null"]},
                            "Rec_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["Rec_Nombre"],
                    },
                },
                "bd_gastos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Gasto_Objeto": {"type": "string"},
                            "Gasto_Categoria": {"type": ["string", "null"]},
                            "Gasto_Vigente": {"type": ["number", "null"]},
                            "Gasto_Preventivo": {"type": ["number", "null"]},
                            "Gasto_Compromiso": {"type": ["number", "null"]},
                            "Gasto_Devengado": {"type": ["number", "null"]},
                            "Gasto_Pagado": {"type": ["number", "null"]},
                            "Gasto_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["Gasto_Objeto"],
                    },
                },
                "bd_jurisdiccion": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Juri_Codigo": {"type": ["string", "null"]},
                            "Juri_Nombre": {"type": ["string", "null"]},
                            "Juri_Descripcion": {"type": ["string", "null"]},
                            "Juri_Orden": {"type": ["number", "null"]},
                            "Juri_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["Juri_Codigo"],
                    },
                },
                "bd_programas": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Juri_Codigo": {"type": ["string", "null"]},
                            "Prog_Codigo": {"type": ["string", "null"]},
                            "Prog_Nombre": {"type": "string"},
                            "Prog_Vigente": {"type": ["number", "null"]},
                            "Prog_Preventivo": {"type": ["number", "null"]},
                            "Prog_Compromiso": {"type": ["number", "null"]},
                            "Prog_Devengado": {"type": ["number", "null"]},
                            "Prog_Pagado": {"type": ["number", "null"]},
                            "Prog_Tipo": {"type": ["string", "null"]},
                            "Prog_TieneMetas": {"type": ["boolean", "null"]},
                            "Prog_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["Prog_Nombre"],
                    },
                },
                "bd_metas": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Juri_Codigo": {"type": ["string", "null"]},
                            "Prog_Codigo": {"type": ["string", "null"]},
                            "Prog_Nombre": {"type": ["string", "null"]},
                            "Meta_Nombre": {"type": "string"},
                            "Meta_Unidad": {"type": ["string", "null"]},
                            "Meta_Anual": {"type": ["number", "null"]},
                            "Meta_Parcial": {"type": ["number", "null"]},
                            "Meta_Ejecutado": {"type": ["number", "null"]},
                            "Meta_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["Meta_Nombre"],
                    },
                },
                "bd_movimientosTesoreria": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "MovTes_Periodo": {"type": ["string", "null"]},
                            "MovTes_Tipo": {"type": "string"},
                            "MovTes_TipoResumido": {"type": ["string", "null"]},
                            "MovTes_Importe": {"type": ["number", "null"]},
                            "MovTes_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["MovTes_Tipo"],
                    },
                },
                "bd_cuentas": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Cuenta_Codigo": {"type": ["string", "null"]},
                            "Cuenta_Nombre": {"type": "string"},
                            "Cuenta_Tipo": {"type": ["string", "null"]},
                            "Cuenta_Importe": {"type": ["number", "null"]},
                        },
                        "required": ["Cuenta_Nombre"],
                    },
                },
                "bd_situacionpatrimonial": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "SitPat_Codigo": {"type": ["string", "null"]},
                            "SitPat_Nombre": {"type": "string"},
                            "SitPat_Tipo": {"type": ["string", "null"]},
                            "SitPat_Saldo": {"type": ["number", "null"]},
                            "SitPat_Observacion": {"type": ["string", "null"]},
                        },
                        "required": ["SitPat_Nombre"],
                    },
                },
                "warnings": {"type": "array", "items": {"type": "string"}},
            },
            "required": [
                "bd_recursos",
                "bd_gastos",
                "bd_jurisdiccion",
                "bd_programas",
                "bd_metas",
                "bd_movimientosTesoreria",
                "bd_cuentas",
                "bd_situacionpatrimonial",
                "warnings",
            ],
        },
    }


def _xlsx_to_text(
    xlsx_path: str, max_rows: int = 300, max_cols: int = 20
) -> str:
    sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, engine="openpyxl")
    parts: list[str] = []
    for name, df in sheets.items():
        df = df.iloc[:max_rows, :max_cols].copy()
        df = df.fillna("").astype(str)
        lines: list[str] = []
        for _, row in df.iterrows():
            values = [str(v).replace("\n", " ").replace("\r", " ").strip() for v in row.tolist()]
            lines.append(" | ".join(values))
        parts.append(f"### {name}\n" + "\n".join(lines))
    return "\n\n".join(parts)


def extract_xlsx_single_shot(
    client: OpenAI,
    model: str,
    xlsx_path: str,
    max_retries: int,
    retry_sleep_sec: float,
) -> dict[str, Any]:
    schema = build_schema()
    schema_payload = schema.get("schema", schema)
    last_error: Exception | None = None
    text_all = _xlsx_to_text(xlsx_path)
    use_responses = hasattr(client, "responses")
    for attempt in range(max_retries + 1):
        try:
            if use_responses:
                try:
                    response = client.responses.create(
                        model=model,
                        input=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {
                                "role": "user",
                                "content": f"{USER_PROMPT}\n\nContenido del XLSX:\n{text_all}",
                            },
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
                f"{USER_PROMPT}\n\n"
                "Contenido del XLSX:\n"
                f"{text_all}\n\n"
                "Devuelve SOLO JSON valido y estricto segun este JSON Schema:\n"
                f"{schema_text}"
            )
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
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
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt < max_retries:
                time.sleep(retry_sleep_sec * (attempt + 1))
            else:
                raise RuntimeError(
                    f"LLM fallo despues de reintentos: {last_error}"
                ) from last_error
    raise RuntimeError("LLM fallo.") from last_error
