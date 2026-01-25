from __future__ import annotations

import json
from typing import Any

from postgrest.exceptions import APIError
from supabase import Client, create_client


_TABLE_FALLBACKS = {
    "BD_Jurisdicciones": ["bd_jurisdiccion"],
    "BD_Programas": ["bd_programas"],
    "BD_Metas": ["bd_metas"],
}


def _get_table(client: Client, name: str):
    return client.table(name)


def _with_fallback(name: str) -> list[str]:
    return [name] + _TABLE_FALLBACKS.get(name, [])


def build_client(url: str, key: str) -> Client:
    if not url or not key:
        raise ValueError("Supabase URL/KEY faltantes.")
    return create_client(url, key)


def create_document(
    client: Client,
    municipio: str,
    periodo: str,
    tipo: str,
    extra: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "Municipio": municipio,
        "Periodo": periodo,
        "Tipo": tipo,
        "Estado": "iniciado",
    }
    if extra:
        payload.update(extra)
    response = client.table("BD_DocumentosCargados").insert(payload).execute()
    if not response.data:
        raise RuntimeError("No se pudo crear BD_DocumentosCargados.")
    row = response.data[0]
    return row.get("ID_DocumentoCargado") or row.get("id")


def update_document_status(client: Client, doc_id: str, estado: str, resumen: dict[str, Any]) -> None:
    payload = {
        "Estado": estado,
        "ResumenCarga": json.dumps(resumen, ensure_ascii=True),
    }
    client.table("BD_DocumentosCargados").update(payload).eq("ID_DocumentoCargado", doc_id).execute()


def upsert_jurisdicciones(client: Client, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    for name in _with_fallback("BD_Jurisdicciones"):
        table = _get_table(client, name)
        try:
            table.upsert(rows, on_conflict="ID_DocumentoCargado,Juri_Codigo").execute()
            return
        except TypeError:
            try:
                table.upsert(rows).execute()
                return
            except APIError as exc:
                if exc.code != "PGRST205":
                    raise
        except APIError as exc:
            if exc.code != "PGRST205":
                raise


def upsert_programas(client: Client, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    for name in _with_fallback("BD_Programas"):
        table = _get_table(client, name)
        try:
            table.upsert(
                rows,
                on_conflict="ID_DocumentoCargado,Juri_Codigo,Prog_Codigo",
            ).execute()
            return
        except TypeError:
            try:
                table.upsert(rows).execute()
                return
            except APIError as exc:
                if exc.code != "PGRST205":
                    raise
        except APIError as exc:
            if exc.code != "PGRST205":
                raise


def upsert_metas(client: Client, rows: list[dict[str, Any]], on_conflict: str) -> None:
    if not rows:
        return
    for name in _with_fallback("BD_Metas"):
        table = _get_table(client, name)
        try:
            table.upsert(rows, on_conflict=on_conflict).execute()
            return
        except TypeError:
            try:
                table.upsert(rows).execute()
                return
            except APIError as exc:
                if exc.code != "PGRST205":
                    raise
        except APIError as exc:
            if exc.code != "PGRST205":
                raise


def insert_metas_staging(client: Client, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    client.table(table).insert(rows).execute()


def fetch_programas_mapping(client: Client, doc_id: str) -> list[dict[str, Any]]:
    last_error: APIError | None = None
    for name in _with_fallback("BD_Programas"):
        try:
            response = (
                client.table(name)
                .select("ID_Programa,Prog_Codigo,Juri_Codigo,Prog_Nombre")
                .eq("ID_DocumentoCargado", doc_id)
                .execute()
            )
            return response.data or []
        except APIError as exc:
            last_error = exc
            if exc.code != "PGRST205":
                raise
    if last_error:
        raise last_error
    return []
