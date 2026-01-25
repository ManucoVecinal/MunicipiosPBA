# -*- coding: utf-8 -*-
"""
Ingesta del PDF: valida, calcula hash, sube a Storage y registra el documento.
"""
from __future__ import annotations

from typing import Any, Dict

from .validate import validate_pdf_bytes
from .utils import compute_sha256, utc_now_iso
from .load_supabase import upload_pdf_to_storage, insert_document_record


DEFAULT_BUCKET = "pdfs"
DEFAULT_ESTADO = "Pendiente"


def ingest_pdf(
    supabase,
    pdf_bytes: bytes,
    filename: str,
    metadata_dict: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Orquesta la ingesta inicial del PDF.

    Retorna un dict consistente:
    {ok: bool, doc_id: str|int|None, storage_path: str|None, hash: str|None, error: str|None}
    """
    result = {
        "ok": False,
        "doc_id": None,
        "storage_path": None,
        "hash": None,
        "error": None,
    }

    try:
        # Validaciones basicas
        validate_pdf_bytes(pdf_bytes)

        if not filename:
            raise ValueError("El nombre de archivo esta vacio.")

        if not isinstance(metadata_dict, dict):
            raise ValueError("metadata_dict debe ser un dict con metadatos del documento.")

        # Hash y nombre
        file_hash = compute_sha256(pdf_bytes)
        size_bytes = len(pdf_bytes)

        # Armar storage_path estable y deduplicable
        # Ejemplo: documentos/{ID_Municipio}/{hash}.pdf
        muni_id = metadata_dict.get("ID_Municipio")
        if not muni_id:
            raise ValueError("metadata_dict debe incluir ID_Municipio.")

        storage_path = f"documentos/{muni_id}/{file_hash}.pdf"

        # Subida a Storage
        bucket = metadata_dict.get("bucket", DEFAULT_BUCKET)
        upload_pdf_to_storage(supabase, bucket, storage_path, pdf_bytes)

        # Registro en BD_DocumentosCargados
        now = utc_now_iso()
        data = {
            # Campos funcionales ya existentes en tu app
            "ID_Municipio": metadata_dict.get("ID_Municipio"),
            "Doc_Tipo": metadata_dict.get("Doc_Tipo"),
            "Doc_Periodo": metadata_dict.get("Doc_Periodo"),
            "Doc_Anio": metadata_dict.get("Doc_Anio"),
            "Doc_Nombre": metadata_dict.get("Doc_Nombre"),
            # Trazabilidad de archivo
            "Doc_ArchivoNombreOriginal": filename,
            "Doc_ArchivoSizeBytes": size_bytes,
            "Doc_ArchivoHash": file_hash,
            "Doc_ArchivoStoragePath": storage_path,
            # Estado y timestamps
            "Doc_Estado": metadata_dict.get("Doc_Estado", DEFAULT_ESTADO),
            "Doc_CreadoUTC": now,
            "Doc_ActualizadoUTC": now,
        }

        doc_id = insert_document_record(supabase, data)

        result.update(
            {
                "ok": True,
                "doc_id": doc_id,
                "storage_path": storage_path,
                "hash": file_hash,
            }
        )
        return result

    except Exception as e:
        result["error"] = str(e)
        return result
