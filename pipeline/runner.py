# -*- coding: utf-8 -*-
"""
Runner manual para procesar un documento ya ingestado.
"""
from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

from .load_supabase import (
    FIELD_ID,
    FIELD_STORAGE_PATH,
    fetch_documents,
    update_document_status,
    download_pdf_from_storage,
    insert_rows,
    fetch_rows,
    delete_rows_by_filters,
    delete_rows,
)
from .parsers.recursos import parse_recursos_from_text
from .parsers.gastos import parse_gastos_objeto_from_text
from .parsers.programas import parse_programas_from_text
from .parsers.movimientos import parse_movimientos_from_text, extract_periodo
from .parsers.cuentas import parse_cuentas_from_text
from .parsers.sitpat import parse_sitpat_from_text
from .parsers.metas import parse_metas_from_text

DEFAULT_BUCKET = "pdfs"

def _extract_text_with_pdfplumber(pdf_bytes: bytes) -> str:
    """
    Extrae texto completo con pdfplumber.
    """
    try:
        import pdfplumber
    except Exception as e:
        raise RuntimeError(f"pdfplumber no esta disponible: {e}") from e

    all_text: List[str] = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            all_text.append(text)
    return "\n".join(all_text)


def run_document(supabase, doc_id: Any) -> Dict[str, Any]:
    """
    Ejecuta el procesamiento manual de un documento por ID.

    Retorna un dict consistente:
    {ok: bool, metrics: dict|None, error: str|None}
    """
    result = {"ok": False, "metrics": None, "error": None}

    try:
        if not doc_id:
            raise ValueError("doc_id esta vacio.")

        docs = fetch_documents(supabase, {FIELD_ID: doc_id})
        if not docs:
            raise ValueError("No se encontro el documento solicitado.")

        doc = docs[0]
        storage_path = doc.get(FIELD_STORAGE_PATH)
        if not storage_path:
            raise ValueError("El documento no tiene storage_path.")

        # 1) Marcar como procesando
        update_document_status(supabase, doc_id, "Procesando")

        # 2) Descargar PDF
        pdf_bytes = download_pdf_from_storage(supabase, DEFAULT_BUCKET, storage_path)

        doc_tipo = str(doc.get("Doc_Tipo", "") or "")
        doc_nombre = str(doc.get("Doc_Nombre", "") or "")
        tipo_norm = doc_tipo.upper().replace("-", "")
        nombre_norm = doc_nombre.upper().replace("-", "")
        is_sit_eco = ("SITECO" in tipo_norm) or ("SITECO" in nombre_norm)
        if not is_sit_eco:
            raise ValueError("Tipo de documento no soportado para este runner.")

        # 3) Extraer texto
        text_all = _extract_text_with_pdfplumber(pdf_bytes)

        # 4) Parsear recursos
        warnings: List[str] = []
        recursos = parse_recursos_from_text(text_all, warnings=warnings)

        # 5) Parsear gastos por objeto
        gastos = parse_gastos_objeto_from_text(text_all, warnings=warnings)

        # 6) Parsear jurisdicciones y programas
        prog_result = parse_programas_from_text(text_all, warnings=warnings)
        jurisdicciones = prog_result["jurisdicciones"]
        programas = prog_result["programas"]

        # 7) Reemplazar e insertar en bd_recursos
        id_muni = doc.get("ID_Municipio")
        rows = []
        for r in recursos:
            row = dict(r)
            row["ID_DocumentoCargado"] = doc_id
            if id_muni is not None:
                row["ID_Municipio"] = id_muni
            rows.append(row)

        delete_rows_by_filters(
            supabase, "bd_recursos", {"ID_DocumentoCargado": doc_id}
        )
        rec_inserted = insert_rows(supabase, "bd_recursos", rows)

        # 8) Reemplazar e insertar en bd_gastos
        gasto_rows = []
        for g in gastos:
            row = dict(g)
            row["ID_DocumentoCargado"] = doc_id
            if id_muni is not None:
                row["ID_Municipio"] = id_muni
            gasto_rows.append(row)
        delete_rows_by_filters(
            supabase, "bd_gastos", {"ID_DocumentoCargado": doc_id}
        )
        gasto_inserted = insert_rows(supabase, "bd_gastos", gasto_rows)

        # 9) Reemplazar e insertar jurisdicciones
        juri_rows = []
        seen_juri = set()
        for j in jurisdicciones:
            name = j.get("Juri_Nombre")
            code = j.get("Juri_Codigo")
            key = code or name
            if not key or key in seen_juri:
                continue
            seen_juri.add(key)
            row = dict(j)
            row["ID_DocumentoCargado"] = doc_id
            if id_muni is not None:
                row["ID_Municipio"] = id_muni
            juri_rows.append(row)
        delete_rows_by_filters(
            supabase, "bd_jurisdiccion", {"ID_DocumentoCargado": doc_id}
        )
        insert_rows(supabase, "bd_jurisdiccion", juri_rows)

        # 10) Mapear jurisdicciones a IDs
        juri_map = {}
        juri_db = fetch_rows(
            supabase, "bd_jurisdiccion", {"ID_DocumentoCargado": doc_id}
        )
        for j in juri_db:
            name = j.get("Juri_Nombre")
            code = j.get("Juri_Codigo")
            if code:
                juri_map[code] = j.get("ID_Jurisdiccion")
            elif name:
                juri_map[name] = j.get("ID_Jurisdiccion")

        # 11) Reemplazar e insertar programas
        prog_rows = []
        for p in programas:
            juri_code = p.get("Juri_Codigo")
            juri_name = p.get("Juri_Nombre")
            juri_id = None
            if juri_code:
                juri_id = juri_map.get(juri_code)
            if not juri_id and juri_name:
                juri_id = juri_map.get(juri_name)
            if not juri_id:
                warnings.append(f"Programa sin jurisdiccion: '{p.get('Prog_Nombre')}'")
                continue
            row = dict(p)
            row.pop("Juri_Nombre", None)
            row.pop("Juri_Codigo", None)
            row["ID_Jurisdiccion"] = juri_id
            prog_rows.append(row)
        if juri_map:
            delete_rows_by_filters(
                supabase, "bd_programas", {"ID_Jurisdiccion": list(juri_map.values())}
            )
        prog_inserted = insert_rows(supabase, "bd_programas", prog_rows)

        # 12) Movimientos de tesoreria
        movs = parse_movimientos_from_text(text_all, warnings=warnings)
        periodo = extract_periodo(text_all)
        mov_rows = []
        for m in movs:
            row = dict(m)
            row["ID_DocumentoCargado"] = doc_id
            if id_muni is not None:
                row["ID_Municipio"] = id_muni
            if periodo:
                row["MovTes_Periodo"] = periodo
            mov_rows.append(row)
        delete_rows_by_filters(
            supabase, "bd_movimientosTesoreria", {"ID_DocumentoCargado": doc_id}
        )
        mov_inserted = insert_rows(supabase, "bd_movimientosTesoreria", mov_rows)

        # 13) Cuentas
        cuentas = parse_cuentas_from_text(text_all, warnings=warnings)
        cuenta_rows = []
        for c in cuentas:
            row = dict(c)
            row["ID_DocumentoCargado"] = doc_id
            if id_muni is not None:
                row["ID_Municipio"] = id_muni
            cuenta_rows.append(row)
        delete_rows_by_filters(
            supabase, "bd_cuentas", {"ID_DocumentoCargado": doc_id}
        )
        cuenta_inserted = insert_rows(supabase, "bd_cuentas", cuenta_rows)

        # 14) Situacion patrimonial
        sitpat = parse_sitpat_from_text(text_all, warnings=warnings)
        sit_rows = []
        for s in sitpat:
            row = dict(s)
            row["ID_DocumentoCargado"] = doc_id
            if id_muni is not None:
                row["ID_Municipio"] = id_muni
            sit_rows.append(row)
        delete_rows_by_filters(
            supabase, "bd_situacionpatrimonial", {"ID_DocumentoCargado": doc_id}
        )
        sit_inserted = insert_rows(supabase, "bd_situacionpatrimonial", sit_rows)

        # 15) Metas
        metas_raw = parse_metas_from_text(text_all, warnings=warnings)
        metas_rows = []
        prog_db = fetch_rows(
            supabase, "bd_programas", {"ID_Jurisdiccion": list(juri_map.values())}
        )
        prog_by_key = {}
        prog_by_name = {}
        for p in prog_db:
            pid = p.get("ID_Programa")
            juri_id = p.get("ID_Jurisdiccion")
            prog_code = str(p.get("Prog_Codigo") or "").strip()
            prog_name = str(p.get("Prog_Nombre") or "").strip().lower()
            juri_code = None
            for code, jid in juri_map.items():
                if jid == juri_id:
                    juri_code = code
                    break
            if juri_code:
                if prog_code:
                    prog_by_key[(juri_code, prog_code)] = pid
                if prog_name:
                    prog_by_name[(juri_code, prog_name)] = pid

        for m in metas_raw:
            juri_code = str(m.get("Juri_Codigo") or "").strip()
            prog_code = str(m.get("Prog_Codigo") or "").strip()
            prog_name = str(m.get("Prog_Nombre") or "").strip().lower()
            prog_id = None
            if juri_code and prog_code:
                prog_id = prog_by_key.get((juri_code, prog_code))
            if not prog_id and juri_code and prog_name:
                prog_id = prog_by_name.get((juri_code, prog_name))
            if not prog_id:
                warnings.append(
                    f"No se encontro programa para meta: {juri_code} {prog_code} {m.get('Meta_Nombre')}"
                )
                continue
            metas_rows.append(
                {
                    "ID_Programa": prog_id,
                    "Meta_Nombre": m.get("Meta_Nombre"),
                    "Meta_Unidad": m.get("Meta_Unidad"),
                    "Meta_Anual": m.get("Meta_Anual"),
                    "Meta_Parcial": m.get("Meta_Parcial"),
                    "Meta_Ejecutado": m.get("Meta_Ejecutado"),
                    "Meta_Observacion": m.get("Meta_Observacion"),
                }
            )

        if metas_rows:
            delete_rows(
                supabase,
                "bd_metas",
                "ID_Programa",
                list({row["ID_Programa"] for row in metas_rows}),
            )
        meta_inserted = insert_rows(supabase, "bd_metas", metas_rows)

        if metas_rows:
            try:
                supabase.table("bd_programas").update({"Prog_TieneMetas": True}).in_(
                    "ID_Programa", list({row["ID_Programa"] for row in metas_rows})
                ).execute()
            except Exception:
                warnings.append("No se pudo actualizar Prog_TieneMetas.")

        metrics = {
            "recursos_rows_inserted": rec_inserted,
            "gastos_rows_inserted": gasto_inserted,
            "jurisdicciones_rows_inserted": len(juri_rows),
            "programas_rows_inserted": prog_inserted,
            "tesoreria_rows_inserted": mov_inserted,
            "cuentas_rows_inserted": cuenta_inserted,
            "sitpat_rows_inserted": sit_inserted,
            "metas_rows_inserted": meta_inserted,
            "warnings": warnings,
        }

        # 6) OK
        update_document_status(supabase, doc_id, "Procesado")

        result["ok"] = True
        result["metrics"] = metrics
        return result

    except Exception as e:
        result["error"] = str(e)
        try:
            update_document_status(supabase, doc_id, "Error", error_msg=str(e))
        except Exception:
            pass
        return result
