from __future__ import annotations

from typing import Any

from postgrest.exceptions import APIError
from supabase import Client, create_client


def build_client(url: str, key: str) -> Client:
    if not url or not key:
        raise ValueError("Supabase URL/KEY faltantes.")
    return create_client(url, key)


def create_document(
    client: Client,
    id_municipio: str,
    doc_nombre: str,
    doc_tipo: str,
    doc_periodo: str,
) -> str:
    payload = {
        "ID_Municipio": id_municipio,
        "Doc_Nombre": doc_nombre,
        "Doc_Tipo": doc_tipo,
        "Doc_Periodo": doc_periodo,
    }
    response = client.table("BD_DocumentosCargados").insert(payload).execute()
    if not response.data:
        raise RuntimeError("No se pudo crear BD_DocumentosCargados.")
    row = response.data[0]
    return row.get("ID_DocumentoCargado") or row.get("id")


def _upsert(table, rows: list[dict[str, Any]], on_conflict: str | None = None) -> None:
    if not rows:
        return
    try:
        if on_conflict:
            table.upsert(rows, on_conflict=on_conflict).execute()
        else:
            table.upsert(rows).execute()
    except TypeError:
        table.upsert(rows).execute()
    except APIError as exc:
        # If the target table has no unique constraint matching on_conflict,
        # fall back to a plain insert to avoid hard failure.
        if getattr(exc, "code", None) == "42P10":
            table.insert(rows).execute()
        else:
            raise


def upsert_cuentas(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(client.table("bd_cuentas"), rows, on_conflict="ID_DocumentoCargado,Cuenta_Codigo")


def upsert_gastos(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(
        client.table("bd_gastos"),
        rows,
        on_conflict="ID_DocumentoCargado,Gasto_Categoria,Gasto_Objeto",
    )


def upsert_recursos(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(
        client.table("bd_recursos"),
        rows,
        on_conflict="ID_DocumentoCargado,Rec_Categoria,Rec_Tipo",
    )


def upsert_jurisdicciones(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(client.table("bd_jurisdiccion"), rows, on_conflict="ID_DocumentoCargado,Juri_Codigo")


def upsert_programas(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(client.table("bd_programas"), rows, on_conflict="ID_Jurisdiccion,Prog_Codigo")


def upsert_movimientos(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(client.table("bd_movimientosTesoreria"), rows, on_conflict="ID_DocumentoCargado,MovTes_Tipo")


def upsert_sitpat(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(
        client.table("bd_situacionpatrimonial"),
        rows,
        on_conflict="ID_DocumentoCargado,SitPat_Tipo,SitPat_Nombre",
    )


def upsert_metas(client: Client, rows: list[dict[str, Any]]) -> None:
    _upsert(client.table("bd_metas"), rows, on_conflict="ID_Programa,Meta_Nombre")


def fetch_jurisdicciones(client: Client, doc_id: str) -> list[dict[str, Any]]:
    response = (
        client.table("bd_jurisdiccion")
        .select("ID_Jurisdiccion,Juri_Codigo,Juri_Nombre")
        .eq("ID_DocumentoCargado", doc_id)
        .execute()
    )
    return response.data or []


def fetch_programas_for_juris(client: Client, juri_ids: list[str]) -> list[dict[str, Any]]:
    if not juri_ids:
        return []
    response = (
        client.table("bd_programas")
        .select("ID_Programa,Prog_Codigo,Prog_Nombre,ID_Jurisdiccion")
        .in_("ID_Jurisdiccion", juri_ids)
        .execute()
    )
    return response.data or []


def insert_metas_staging(client: Client, table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    client.table(table).insert(rows).execute()


def update_document_status(
    client: Client,
    doc_id: str,
    estado: str,
    resumen: dict[str, Any],
) -> None:
    try:
        client.table("BD_DocumentosCargados").update(
            {"Doc_EstadoCarga": estado, "Doc_ResumenCarga": json_dumps(resumen)}
        ).eq("ID_DocumentoCargado", doc_id).execute()
    except APIError:
        pass


def json_dumps(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=True)
