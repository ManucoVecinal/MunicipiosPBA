# -*- coding: utf-8 -*-
"""
Funciones de acceso a Supabase para el pipeline.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .utils import utc_now_iso

# Campos esperados en BD_DocumentosCargados.
# Ajustar estos nombres si tu tabla usa otros.
DOCUMENT_FIELDS = [
    "ID_DocumentoCargado",
    "ID_Municipio",
    "Doc_Tipo",
    "Doc_Periodo",
    "Doc_Anio",
    "Doc_Nombre",
    "Doc_ArchivoNombreOriginal",
    "Doc_ArchivoSizeBytes",
    "Doc_ArchivoHash",
    "Doc_ArchivoStoragePath",
    "Doc_Estado",
    "Doc_Error",
    "Doc_CreadoUTC",
    "Doc_ActualizadoUTC",
]

TABLE_DOCUMENTS = "BD_DocumentosCargados"
FIELD_ID = "ID_DocumentoCargado"
FIELD_STATUS = "Doc_Estado"
FIELD_ERROR = "Doc_Error"
FIELD_STORAGE_PATH = "Doc_ArchivoStoragePath"
FIELD_UPDATED = "Doc_ActualizadoUTC"


def _get_storage_client(supabase):
    """
    Devuelve el cliente de Storage para distintas versiones del SDK.
    """
    storage = getattr(supabase, "storage", None)
    if storage is None:
        raise RuntimeError("El cliente de Supabase no tiene atributo storage.")
    if callable(storage):
        storage = storage()
    return storage


def upload_pdf_to_storage(
    supabase,
    bucket: str,
    storage_path: str,
    pdf_bytes: bytes,
    content_type: str = "application/pdf",
) -> str:
    """
    Sube el PDF a Supabase Storage y devuelve el path.
    """
    if not bucket:
        raise ValueError("bucket esta vacio.")
    if not storage_path:
        raise ValueError("storage_path esta vacio.")

    storage = _get_storage_client(supabase)
    options_list = [
        {"content-type": content_type, "upsert": "true"},
        {"content-type": content_type, "upsert": True},
        {"content-type": content_type},
    ]

    last_error = None
    res = None
    for options in options_list:
        try:
            res = storage.from_(bucket).upload(
                storage_path,
                pdf_bytes,
                options,
            )
            last_error = None
            break
        except Exception as e:
            msg = str(e)
            if "Duplicate" in msg or "already exists" in msg:
                return storage_path
            last_error = e
            continue

    if last_error is not None:
        raise RuntimeError(f"Error al subir a Storage: {last_error}") from last_error

    # En algunos SDKs, res tiene atributos; en otros, es dict.
    # Si hubo error, solemos ver 'error' o status_code.
    if isinstance(res, dict) and res.get("error"):
        err = res.get("error")
        message = str(res.get("message", ""))
        if err == "Duplicate" or "already exists" in message:
            return storage_path
        raise RuntimeError(f"Error en Storage: {res['error']}")

    return storage_path


def insert_document_record(supabase, data: dict) -> Any:
    """
    Inserta un registro en BD_DocumentosCargados y devuelve el ID generado.
    """
    if not isinstance(data, dict):
        raise ValueError("data debe ser un dict con los campos del documento.")

    try:
        res = supabase.table(TABLE_DOCUMENTS).insert(data).execute()
    except Exception as e:
        raise RuntimeError(f"Error al insertar en BD_DocumentosCargados: {e}") from e

    # Manejar respuesta del SDK
    record = None
    if hasattr(res, "data"):
        record = res.data[0] if res.data else None
    elif isinstance(res, dict):
        data_val = res.get("data")
        if isinstance(data_val, list) and data_val:
            record = data_val[0]

    if not record:
        raise RuntimeError("No se pudo obtener el ID del documento insertado.")

    # Ajustar aqui si tu PK tiene otro nombre
    doc_id = record.get(FIELD_ID)
    if not doc_id:
        raise RuntimeError("El registro insertado no devolvio ID_DocumentoCargado.")

    return doc_id


def insert_rows(supabase, table_name: str, rows: List[Dict[str, Any]]) -> int:
    """
    Inserta multiples filas y devuelve la cantidad insertada.
    """
    if not table_name:
        raise ValueError("table_name esta vacio.")
    if rows is None:
        raise ValueError("rows es None.")
    if not rows:
        return 0

    # Normalizar: todas las filas deben tener las mismas keys
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    normalized_rows = []
    for row in rows:
        normalized = {k: row.get(k, None) for k in all_keys}
        normalized_rows.append(normalized)

    try:
        res = supabase.table(table_name).insert(normalized_rows).execute()
    except Exception as e:
        raise RuntimeError(f"Error al insertar en {table_name}: {e}") from e

    if hasattr(res, "data") and isinstance(res.data, list):
        return len(res.data)
    if isinstance(res, dict) and isinstance(res.get("data"), list):
        return len(res.get("data"))
    return 0


def delete_rows(supabase, table_name: str, pk_col: str, ids: List[Any]) -> int:
    """
    Borra filas por PK y devuelve la cantidad eliminada.
    """
    if not table_name:
        raise ValueError("table_name esta vacio.")
    if not pk_col:
        raise ValueError("pk_col esta vacio.")
    if not ids:
        return 0

    # Normalizar IDs (uuid) a string y filtrar vacios
    normalized_ids = []
    for val in ids:
        if val is None:
            continue
        val_str = str(val).strip()
        if not val_str or val_str.lower() == "nan":
            continue
        normalized_ids.append(val_str)

    if not normalized_ids:
        return 0

    try:
        res = supabase.table(table_name).delete().in_(pk_col, normalized_ids).execute()
    except Exception as e:
        raise RuntimeError(f"Error al borrar en {table_name}: {e}") from e

    # Manejar errores del SDK si vienen en la respuesta
    if hasattr(res, "error") and res.error:
        raise RuntimeError(f"Error al borrar en {table_name}: {res.error}")
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(f"Error al borrar en {table_name}: {res['error']}")

    if hasattr(res, "data") and isinstance(res.data, list):
        return len(res.data)
    if isinstance(res, dict) and isinstance(res.get("data"), list):
        return len(res.get("data"))
    return 0


def delete_rows_by_filters(
    supabase, table_name: str, filters: Dict[str, Any]
) -> int:
    """
    Borra filas aplicando filtros simples (eq o in_) y devuelve la cantidad eliminada.
    """
    if not table_name:
        raise ValueError("table_name esta vacio.")
    if not isinstance(filters, dict):
        raise ValueError("filters debe ser un dict.")

    query = supabase.table(table_name).delete()
    for key, value in filters.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            query = query.in_(key, list(value))
        else:
            query = query.eq(key, value)

    try:
        res = query.execute()
    except Exception as e:
        raise RuntimeError(f"Error al borrar en {table_name}: {e}") from e

    if hasattr(res, "error") and res.error:
        raise RuntimeError(f"Error al borrar en {table_name}: {res.error}")
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(f"Error al borrar en {table_name}: {res['error']}")

    if hasattr(res, "data") and isinstance(res.data, list):
        return len(res.data)
    if isinstance(res, dict) and isinstance(res.get("data"), list):
        return len(res.get("data"))
    return 0


def fetch_documents(supabase, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Obtiene documentos aplicando filtros simples (eq o in_).
    """
    if not isinstance(filters, dict):
        raise ValueError("filters debe ser un dict.")

    query = supabase.table(TABLE_DOCUMENTS).select("*")
    for key, value in filters.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            query = query.in_(key, list(value))
        else:
            query = query.eq(key, value)

    try:
        res = query.execute()
    except Exception as e:
        raise RuntimeError(f"Error al consultar documentos: {e}") from e

    if hasattr(res, "data"):
        return res.data or []
    if isinstance(res, dict):
        return res.get("data") or []
    return []


def fetch_rows(supabase, table_name: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Obtiene filas de una tabla aplicando filtros simples (eq o in_).
    """
    if not table_name:
        raise ValueError("table_name esta vacio.")
    if not isinstance(filters, dict):
        raise ValueError("filters debe ser un dict.")

    query = supabase.table(table_name).select("*")
    for key, value in filters.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            query = query.in_(key, list(value))
        else:
            query = query.eq(key, value)

    try:
        res = query.execute()
    except Exception as e:
        raise RuntimeError(f"Error al consultar {table_name}: {e}") from e

    if hasattr(res, "data"):
        return res.data or []
    if isinstance(res, dict):
        return res.get("data") or []
    return []


def update_document_status(
    supabase, doc_id: Any, status: str, error_msg: str | None = None
) -> None:
    """
    Actualiza el estado del documento y el mensaje de error opcional.
    """
    if not doc_id:
        raise ValueError("doc_id esta vacio.")
    if not status:
        raise ValueError("status esta vacio.")

    data = {
        FIELD_STATUS: status,
        FIELD_UPDATED: utc_now_iso(),
        FIELD_ERROR: error_msg,
    }

    try:
        supabase.table(TABLE_DOCUMENTS).update(data).eq(FIELD_ID, doc_id).execute()
    except Exception as e:
        raise RuntimeError(f"Error al actualizar estado del documento: {e}") from e


def download_pdf_from_storage(
    supabase, bucket: str, storage_path: str
) -> bytes:
    """
    Descarga el PDF desde Storage y devuelve los bytes.
    """
    if not bucket:
        raise ValueError("bucket esta vacio.")
    if not storage_path:
        raise ValueError("storage_path esta vacio.")

    try:
        storage = _get_storage_client(supabase)
        res = storage.from_(bucket).download(storage_path)
    except Exception as e:
        raise RuntimeError(f"Error al descargar desde Storage: {e}") from e

    if isinstance(res, (bytes, bytearray)):
        return bytes(res)
    if hasattr(res, "data") and isinstance(res.data, (bytes, bytearray)):
        return bytes(res.data)
    if hasattr(res, "content") and isinstance(res.content, (bytes, bytearray)):
        return bytes(res.content)

    raise RuntimeError("No se pudieron obtener bytes del PDF descargado.")


def delete_pdf_from_storage(supabase, bucket: str, storage_path: str) -> None:
    """
    Borra un PDF desde Storage.
    """
    if not bucket:
        raise ValueError("bucket esta vacio.")
    if not storage_path:
        raise ValueError("storage_path esta vacio.")

    storage = _get_storage_client(supabase)
    try:
        res = storage.from_(bucket).remove([storage_path])
    except Exception as e:
        raise RuntimeError(f"Error al borrar PDF de Storage: {e}") from e

    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(f"Error en Storage: {res['error']}")
