from __future__ import annotations

import json
import time
from typing import Any

from jsonschema import validate as json_validate
from openai import OpenAI


SYSTEM_PROMPT = """Sos un asistente de extracción de datos contables desde PDFs. Tu tarea es convertir tablas del PDF a JSON EXACTO según el schema. No inventes filas. Si un dato no está, dejalo vacío o null. Ignorá totales, encabezados y subtítulos. Normalizá números: "1.234,56" => 1234.56; "(123,45)" => -123.45. No devuelvas texto fuera del JSON."""

USER_PROMPT = """Voy a darte un PDF. Necesito que actues como data entry humano y cargues estas tablas (arrays) segun reglas:

1) bd_movimientosTesoreria:
Tabla del PDF: "Movimientos de Tesoreria".
Siempre 6 registros:
"Saldo Inicial", "Ingresos del periodo", "Ingresos de Ajustes Contables", "Gastos del Periodo", "Egresos de Ajustes Contables", "Saldo final".
Extraer Importe y cargar:
MovTes_Tipo (texto exactamente como aparece o normalizado a estas 6 etiquetas)
MovTes_Importe (number)

2) bd_cuentas:
La tabla bd_cuentas debe cargarse exclusivamente a partir de la seccion "Demostracion de Saldo", dentro del cuadro "Movimientos de Tesoreria".
Esta seccion esta en la pagina 2 del PDF y enumera cuentas de tesoreria con saldo final.
No usar datos de la tabla "Estado de Situacion Patrimonial" aunque este en la misma pagina.
Reconocer la seccion correcta porque cada fila:
- empieza con un codigo numerico => Cuenta_Codigo
- luego el texto completo de la cuenta => Cuenta_Nombre
- termina con el importe => Cuenta_Importe (number)
La tabla inicia con el registro "CAJA" que viene sin codigo; en ese caso dejar Cuenta_Codigo vacio y usar "CAJA" como Cuenta_Nombre.
Descartar filas con terminos ACTIVO, PASIVO, PATRIMONIO, CORRIENTE/NO CORRIENTE.
No cargar titulos, encabezados, subtotales, totales o filas sin importe.
Si no se identifica con claridad "Demostracion de Saldo", dejar bd_cuentas vacio.

3) bd_gastos:
La tabla bd_gastos debe cargarse a partir de la seccion "Evolucion de Gastos por Objeto" con formato de tabla dinamica.
Categorias: "1. Presupuestarios" y "2. Extrapresupuestarios" => Gasto_Categoria.
Para "1. Presupuestarios", cargar filas para cada objeto:
"Gastos en Personal", "Bienes de consumo", "Servicios no personales", "Bienes de uso", "Transferencias", "Activos financieros", "Servicio de la deuda y disminucion de otros pasivos", "Otros Gastos".
Cargar montos completos: Gasto_Vigente, Gasto_Preventivo, Gasto_Compromiso, Gasto_Devengado, Gasto_Pagado.
Para "2. Extrapresupuestarios", no hay desglose por objeto: cargar un unico registro con Gasto_Objeto = "Extrapresupuestario" y SOLO Gasto_Devengado y Gasto_Pagado (las otras columnas en null).
No cargar encabezados, totales, subtotales ni filas estructurales del formato dinamico.

4) bd_recursos:
La tabla BD_Recursos debe cargarse exclusivamente a partir de "Evolucion de los Recursos".
Filas principales: "1. Presupuestarios" y "2. Extrapresupuestarios" => Rec_Categoria.
Para Presupuestarios, cargar recursos:
"Ingresos corrientes", "Recursos de capital", "Fuentes financieras", "De libre disponibilidad", "Afectados".
El nombre va en Rec_Nombre (texto exacto). Evitar filas "Total" o "Total General (1+2)".
Montos: Rec_Vigente, Rec_Devengado, Rec_Percibido.
Para Extrapresupuestarios: cargar un unico registro con Rec_Nombre = "Extrapresupuestario" y SOLO Rec_Percibido (Rec_Vigente/Rec_Devengado en null).
Ignorar tabla lateral "Cuenta Ahorro Inversion Financiamiento" y sus filas (I..X).

5) bd_jurisdiccion y 6) bd_programas (MISMA tabla):
Tabla del PDF: "Evolucion de Gastos por Programa".
Es tabla dinamica con filas principales:
"Departamento Ejecutivo" y "H.C.D."
Dentro hay programas.
- Jurisdicciones:
Identificar jurisdicciones por su codigo numeral de inicio.
Tambien incluir jurisdicciones especiales:
"Partidas No Asignables a programas" y "Actividades Centrales"
que pueden existir en Departamento Ejecutivo y en H.C.D.
Algunas jurisdicciones pueden repetirse (mismo codigo): deduplicar.
Como el nombre real de jurisdicciones numerales debe inferirse por programas asociados:
- Para cada jurisdiccion numeral, proponer Juri_Nombre inferido por los nombres de sus programas (si no podes, dejalo vacio).
Campos jurisdiccion:
Juri_Codigo (si existe; para especiales usar un codigo textual estable, ej "SINPROG" o "ACTCENT_EXEC" etc.)
Juri_Nombre (inferido o vacio)
Juri_Grupo ("Departamento Ejecutivo" o "H.C.D." si aplica)

- Programas:
Cada fila de programa puede iniciar con:
{Juri_Codigo} + " -" + {Prog_Codigo} + {Prog_Nombre}
o puede ser especial: "Actividades Centrales" / "Partidas no asignables a programas"
Campos programa:
Prog_Codigo (si no existe, dejar "")
Prog_Nombre
Juri_Codigo (o el codigo especial correspondiente)
Prog_Vigente, Prog_Preventivo, Prog_Compromiso, Prog_Devengado, Prog_Pagado

7) bd_metas:
La tabla BD_Metas debe cargarse exclusivamente a partir de la seccion "Evolucion de las principales metas de programas".
Es tabla dinamica: primero aparece un agrupador (usualmente "Departamento Ejecutivo") que no se carga.
Los programas se identifican por fila que inicia con codigo de jurisdiccion de 10 digitos, luego codigo de programa y nombre del programa.
Cada vez que aparece ese patron, se inicia un nuevo programa activo para las metas debajo.
Cada meta inicia con codigo numerico, luego nombre; entre parentesis unidad.
Meta_Nombre = texto sin codigo ni parentesis. Meta_Unidad = texto entre parentesis.
Columnas numericas por orden: Meta_Anual, Meta_Parcial, Meta_Ejecutado. La cuarta columna numerica se ignora.
Todas las metas deben quedar vinculadas a un programa. Si una meta aparece sin programa asignado, asumir perdida de contexto o programa inexistente.
En ese caso, verificar si el programa existe; si no existe, crearlo con el encabezado (codigo jurisdiccion 10 digitos, codigo programa y nombre programa) y vincularlo a la jurisdiccion correspondiente.
Si no se puede reconstruir el encabezado, no forzar: saltar hasta el siguiente encabezado de programa (10 digitos) y continuar.
Metas con valores incompletos se cargan igual dejando nulls.

8) bd_situacionpatrimonial:
La tabla bd_situacionpatrimonial debe cargarse exclusivamente a partir de "Estado de Situacion Patrimonial".
Detectar SitPat_Tipo por bloques ACTIVO / PASIVO / PATRIMONIO PUBLICO.
Cargar SOLO estas filas en SitPat_Nombre:
- ACTIVO: "ACTIVO CORRIENTE", "ACTIVO NO CORRIENTE"
- PASIVO: "PASIVO CORRIENTE", "PASIVO NO CORRIENTE"
- PATRIMONIO PUBLICO: "Capital Fiscal", "Resultado de Ejercicios Anteriores", "Resultado del ejercicio", "Resultados afectados a construccion de bienes de dominio publico"
Cargar el valor de la columna Saldo en SitPat_Saldo (number).
No mezclar con Demostracion de Saldo ni otras tablas.

Salida:
Devolve JSON con arrays:
bd_movimientosTesoreria, bd_cuentas, bd_gastos, bd_recursos, bd_jurisdiccion, bd_programas, bd_metas, bd_situacionpatrimonial
y un array warnings[] con textos cortos para:
- secciones no encontradas
- filas dudosas
- metas sin programa
"""

METAS_ONLY_PROMPT = """Extrae SOLO la tabla BD_Metas desde la seccion \"Evolucion de las principales metas de programa\".
Reglas:
- La fila principal \"Departamento Ejecutivo\" es agrupador y no se carga.
- Cada programa inicia con codigo de jurisdiccion de 10 digitos + codigo de programa + nombre de programa.
- Ese programa queda activo para las metas debajo.
- Cada meta inicia con codigo numerico, luego nombre; entre parentesis unidad.
- Meta_Nombre = texto sin codigo ni parentesis. Meta_Unidad = texto entre parentesis.
- Columnas numericas por orden: Meta_Anual, Meta_Parcial, Meta_Ejecutado.
- Si se pierden metas sin programa activo, intentar recuperar con el ultimo programa valido; si no es posible, saltar hasta el proximo programa (10 digitos).
- Metas con valores incompletos se cargan igual dejando nulls.
- Devuelve SOLO JSON valido segun el schema.
"""


def build_schema() -> dict[str, Any]:
    return {
        "name": "single_shot_ingest",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "bd_movimientosTesoreria": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "MovTes_Tipo": {"type": "string"},
                            "MovTes_Importe": {"type": ["number", "null"]},
                        },
                        "required": ["MovTes_Tipo", "MovTes_Importe"],
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
                            "Cuenta_Importe": {"type": ["number", "null"]},
                        },
                        "required": ["Cuenta_Nombre", "Cuenta_Importe"],
                    },
                },
                "bd_gastos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Gasto_Categoria": {"type": ["string", "null"]},
                            "Gasto_Objeto": {"type": "string"},
                            "Gasto_Vigente": {"type": ["number", "null"]},
                            "Gasto_Preventivo": {"type": ["number", "null"]},
                            "Gasto_Compromiso": {"type": ["number", "null"]},
                            "Gasto_Devengado": {"type": ["number", "null"]},
                            "Gasto_Pagado": {"type": ["number", "null"]},
                        },
                        "required": ["Gasto_Objeto"],
                    },
                },
                "bd_recursos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "Rec_Categoria": {"type": ["string", "null"]},
                            "Rec_TipoRecurso": {"type": "string"},
                            "Rec_Vigente": {"type": ["number", "null"]},
                            "Rec_Devengado": {"type": ["number", "null"]},
                            "Rec_Percibido": {"type": ["number", "null"]},
                        },
                        "required": ["Rec_TipoRecurso"],
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
                            "Juri_Grupo": {"type": ["string", "null"]},
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
                            "Prog_Codigo": {"type": ["string", "null"]},
                            "Prog_Nombre": {"type": "string"},
                            "Juri_Codigo": {"type": ["string", "null"]},
                            "Prog_Vigente": {"type": ["number", "null"]},
                            "Prog_Preventivo": {"type": ["number", "null"]},
                            "Prog_Compromiso": {"type": ["number", "null"]},
                            "Prog_Devengado": {"type": ["number", "null"]},
                            "Prog_Pagado": {"type": ["number", "null"]},
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
                            "Meta_Codigo": {"type": ["string", "null"]},
                            "Meta_Nombre": {"type": "string"},
                            "Meta_Unidad": {"type": ["string", "null"]},
                            "Meta_Anual": {"type": ["number", "null"]},
                            "Meta_Parcial": {"type": ["number", "null"]},
                            "Meta_Ejecutado": {"type": ["number", "null"]},
                            "Juri_Codigo": {"type": ["string", "null"]},
                            "Prog_Codigo": {"type": ["string", "null"]},
                            "Prog_Nombre": {"type": ["string", "null"]},
                        },
                        "required": ["Meta_Nombre"],
                    },
                },
                "bd_situacionpatrimonial": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "SitPat_Tipo": {"type": ["string", "null"]},
                            "SitPat_Nombre": {"type": "string"},
                            "SitPat_Saldo": {"type": ["number", "null"]},
                        },
                        "required": ["SitPat_Tipo", "SitPat_Nombre"],
                    },
                },
                "warnings": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "required": [
                "bd_movimientosTesoreria",
                "bd_cuentas",
                "bd_gastos",
                "bd_recursos",
                "bd_jurisdiccion",
                "bd_programas",
                "bd_metas",
                "bd_situacionpatrimonial",
                "warnings",
            ],
        },
    }


def build_metas_schema() -> dict[str, Any]:
    return {
        "name": "metas_only",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "bd_metas": build_schema()["schema"]["properties"]["bd_metas"],
                "warnings": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["bd_metas", "warnings"],
        },
    }


def extract_pdf_single_shot(
    client: OpenAI,
    model: str,
    pdf_path: str,
    max_retries: int,
    retry_sleep_sec: float,
) -> dict[str, Any]:
    schema = build_schema()
    schema_payload = schema.get("schema", schema)
    use_responses = hasattr(client, "responses")
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            if use_responses:
                try:
                    with open(pdf_path, "rb") as handle:
                        upload = client.files.create(file=handle, purpose="assistants")
                    response = client.responses.create(
                        model=model,
                        input=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {
                                "role": "user",
                                "content": [
                                    {"type": "input_file", "file_id": upload.id},
                                    {"type": "text", "text": USER_PROMPT},
                                ],
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

            text_all = _extract_text_with_pdfplumber(pdf_path)
            schema_text = json.dumps(schema_payload, ensure_ascii=True)
            fallback_user = (
                f"{USER_PROMPT}\n\n"
                "Texto del PDF:\n"
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


def extract_metas_only(
    client: OpenAI,
    model: str,
    pdf_path: str,
    max_retries: int,
    retry_sleep_sec: float,
) -> dict[str, Any]:
    schema = build_metas_schema()
    schema_payload = schema.get("schema", schema)
    last_error: Exception | None = None
    text_pages = _extract_text_with_pdfplumber(pdf_path, keywords=["metas", "evolucion"])
    use_responses = hasattr(client, "responses")
    for attempt in range(max_retries + 1):
        try:
            if use_responses:
                try:
                    with open(pdf_path, "rb") as handle:
                        upload = client.files.create(file=handle, purpose="assistants")
                    response = client.responses.create(
                        model=model,
                        input=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {
                                "role": "user",
                                "content": [
                                    {"type": "input_file", "file_id": upload.id},
                                    {"type": "text", "text": METAS_ONLY_PROMPT},
                                ],
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
                f"{METAS_ONLY_PROMPT}\n\n"
                "Texto del PDF (solo paginas de metas):\n"
                f"{text_pages}\n\n"
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
                    f"LLM fallo despues de reintentos (metas only): {last_error}"
                ) from last_error
    raise RuntimeError("LLM fallo metas.") from last_error


def _extract_text_with_pdfplumber(pdf_path: str, keywords: list[str] | None = None) -> str:
    try:
        import pdfplumber
    except Exception as exc:
        raise RuntimeError(f"pdfplumber no esta disponible: {exc}") from exc

    chunks: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if keywords:
                low = text.lower()
                if not any(key in low for key in keywords):
                    continue
            chunks.append(text)
    return "\n".join(chunks)
