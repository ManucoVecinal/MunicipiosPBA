# -*- coding: utf-8 -*-
"""
Ingesta de RAFAM desde XLSX (tablas Table 1-4) a Supabase.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .load_supabase import delete_rows_by_filters, fetch_rows, insert_rows


logger = logging.getLogger(__name__)

TABLE_1 = "Table 1"
TABLE_2 = "Table 2"
TABLE_3 = "Table 3"
TABLE_4 = "Table 4"

MOV_TYPES_ORDER = [
    "Saldo Inicial",
    "Ingresos del Periodo",
    "Ingresos de Ajustes Contables",
    "Gastos del periodo",
    "Egresos de Ajustes Contables",
    "Saldo Final",
]

_PERIODO_RE = re.compile(r"Del\s+\d{2}/\d{2}/\d{4}\s+al\s+\d{2}/\d{2}/\d{4}")
_PROG_RE = re.compile(r"(\d{10})\s*-?\s*(\d+)\s+(.+)")
_META_RE = re.compile(r"^\s*(\d+)\s+(.+)$")
_SITPAT_ITEM_RE = re.compile(r"^(\d[\d\.]*)\s+(.*)$")


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ")
    return " ".join(text.split()).strip()


def _normalize_key(value: Any) -> str:
    text = _normalize_text(value)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return text.lower()


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _normalize_text(value)
    if not text:
        return None
    # Try to parse localized numbers like 1.234,56
    cleaned = re.sub(r"[^\d\-,\.]", "", text)
    if not cleaned:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "")
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _iter_rows(df: pd.DataFrame) -> Iterable[List[Any]]:
    for _, row in df.iterrows():
        yield list(row.values.tolist())


def _first_text_cell(row: List[Any]) -> str:
    for cell in row:
        text = _normalize_text(cell)
        if text:
            return text
    return ""


def _row_numbers(row: List[Any]) -> List[float]:
    nums = []
    for cell in row:
        val = _parse_float(cell)
        if val is None:
            continue
        nums.append(val)
    return nums


def _find_row_index(df: pd.DataFrame, needle: str) -> Optional[int]:
    needle_key = needle.lower()
    for idx, row in enumerate(_iter_rows(df)):
        for cell in row:
            if needle_key in _normalize_key(cell):
                return idx
    return None


def _parse_table1_recursos(df: pd.DataFrame) -> List[Dict[str, Any]]:
    recursos: List[Dict[str, Any]] = []
    start = _find_row_index(df, "evolucion de los recursos")
    if start is None:
        return recursos

    end = _find_row_index(df, "evolucion de gastos por objeto")
    if end is None:
        end = len(df)

    current_tipo = None
    ignore_keys = {
        "evolucion de los recursos",
        "1. presupuestarios",
        "2. extrapresupuestarios",
        "total",
        "total general (1+2)",
        "vigente",
        "devengado",
        "percibido",
    }
    for row in _iter_rows(df.iloc[start + 1 : end]):
        name = _first_text_cell(row)
        if not name:
            continue
        name_key = _normalize_key(name)
        if name_key.startswith("1. presupuestarios"):
            current_tipo = "Presupuestarios"
            continue
        if name_key.startswith("2. extrapresupuestarios"):
            current_tipo = "Extrapresupuestarios"
            continue
        if name_key in ignore_keys or name_key.startswith("total"):
            continue

        nums = _row_numbers(row)
        if len(nums) < 3:
            continue

        categoria = None
        if name_key in {"de libre disponibilidad", "afectados"}:
            categoria = name

        recursos.append(
            {
                "Rec_Nombre": name,
                "Rec_Categoria": categoria,
                "Rec_Vigente": nums[0],
                "Rec_Devengado": nums[1],
                "Rec_Percibido": nums[2],
                "Rec_Tipo": current_tipo,
                "Rec_Observacion": None,
            }
        )

    return recursos


def _parse_table1_gastos(df: pd.DataFrame) -> List[Dict[str, Any]]:
    gastos: List[Dict[str, Any]] = []
    start = _find_row_index(df, "evolucion de gastos por objeto")
    if start is None:
        return gastos

    current_tipo = None
    ignore_keys = {
        "evolucion de gastos por objeto",
        "1. presupuestarios",
        "2. extrapresupuestarios",
        "total",
        "total general (1+2)",
        "vigente",
        "preventivo",
        "compromiso",
        "devengado",
        "pagado",
    }
    for row in _iter_rows(df.iloc[start + 1 :]):
        name = _first_text_cell(row)
        if not name:
            continue
        name_key = _normalize_key(name)
        if name_key.startswith("1. presupuestarios"):
            current_tipo = "Presupuestarios"
            continue
        if name_key.startswith("2. extrapresupuestarios"):
            current_tipo = "Extrapresupuestarios"
            continue
        if name_key in ignore_keys or name_key.startswith("total"):
            continue

        nums = _row_numbers(row)
        if len(nums) < 5:
            continue

        gastos.append(
            {
                "Gasto_Objeto": name,
                "Gasto_Categoria": current_tipo,
                "Gasto_Vigente": nums[0],
                "Gasto_Preventivo": nums[1],
                "Gasto_Compromiso": nums[2],
                "Gasto_Devengado": nums[3],
                "Gasto_Pagado": nums[4],
                "Gasto_Observacion": None,
            }
        )
    return gastos


def _parse_table2_programas(
    df: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    jurisdicciones: Dict[str, Dict[str, Any]] = {}
    programas: List[Dict[str, Any]] = []

    for row in _iter_rows(df):
        row_texts = [_normalize_text(c) for c in row if _normalize_text(c)]
        if not row_texts:
            continue
        if any("departamento ejecutivo" in t.lower() for t in row_texts):
            continue

        match = None
        for text in row_texts:
            match = _PROG_RE.match(text)
            if match:
                break
        if not match:
            continue

        juri_code = match.group(1)
        prog_code = match.group(2)
        prog_name = match.group(3).strip()
        nums = _row_numbers(row)
        if len(nums) < 5:
            logger.warning("Fila de programa incompleta: %s", match.group(0))
            continue

        if juri_code not in jurisdicciones:
            jurisdicciones[juri_code] = {
                "Juri_Codigo": juri_code,
                "Juri_Nombre": f"Jurisdiccion {juri_code}",
                "Juri_Descripcion": None,
                "Juri_Orden": None,
                "Juri_Observacion": None,
            }

        programas.append(
            {
                "Juri_Codigo": juri_code,
                "Prog_Codigo": prog_code,
                "Prog_Nombre": prog_name,
                "Prog_Vigente": nums[0],
                "Prog_Preventivo": nums[1],
                "Prog_Compromiso": nums[2],
                "Prog_Devengado": nums[3],
                "Prog_Pagado": nums[4],
                "Prog_Tipo": "Presupuestarios",
            }
        )

    return list(jurisdicciones.values()), programas


def _extract_periodo_from_table3(df: pd.DataFrame) -> Optional[str]:
    for row in _iter_rows(df):
        for cell in row:
            text = _normalize_text(cell)
            if not text:
                continue
            match = _PERIODO_RE.search(text)
            if match:
                return match.group(0)
    return None


def _parse_table3_movimientos(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], List[str]]:
    warnings: List[str] = []
    movimientos: List[Dict[str, Any]] = []

    def _normalize_movtes_tipo(value: str) -> Optional[str]:
        key = _normalize_key(value)
        if "saldo" in key and "final" in key:
            return None
        if "saldo" in key and "inicial" in key:
            return "Saldo Inicial"
        if "ingreso" in key:
            return "Ingreso"
        if "gasto" in key or "egreso" in key:
            return "Egreso"
        return None

    start = _find_row_index(df, "movimientos de tesoreria")
    if start is None:
        warnings.append("No se encontro seccion Movimientos de Tesoreria.")
        return movimientos, warnings

    items: List[Tuple[str, Optional[float]]] = []
    for row in _iter_rows(df.iloc[start + 1 :]):
        if len(items) >= len(MOV_TYPES_ORDER):
            break
        name = _normalize_text(row[0] if row else None)
        if not name:
            continue
        amount = _parse_float(row[1] if len(row) > 1 else None)
        if amount is None:
            continue
        label = name.replace(":", "").strip()
        items.append((label, amount))

    for idx, (label, amount) in enumerate(items[: len(MOV_TYPES_ORDER)]):
        tipo_res = _normalize_movtes_tipo(label)
        if not tipo_res:
            continue
        movimientos.append(
            {
                "MovTes_Tipo": label,
                "MovTes_TipoResumido": tipo_res,
                "MovTes_Importe": amount,
            }
        )

        expected = _normalize_key(MOV_TYPES_ORDER[idx])
        if expected not in _normalize_key(label):
            warnings.append(
                f"Orden inesperado en movimientos: esperaba '{MOV_TYPES_ORDER[idx]}' y encontre '{label}'."
            )

    if len(items) < len(MOV_TYPES_ORDER):
        warnings.append(
            "Movimientos de Tesoreria incompletos (esperados 6 tipos)."
        )

    return movimientos, warnings


def _parse_table3_cuentas(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], str]:
    cuentas: List[Dict[str, Any]] = []
    info = ""
    start = _find_row_index(df, "demostracion del saldo")
    if start is None:
        return cuentas, "No se encontro seccion Demostracion del Saldo."

    text_cell = _normalize_text(df.iloc[start, 0] if df.shape[1] > 0 else "")
    if not text_cell:
        return cuentas, "No se encontro texto de cuentas en Demostracion del Saldo."

    matches = list(re.finditer(r"\b\d{9}\b", text_cell))
    cuentas_meta: List[Tuple[str, str]] = []
    for i, match in enumerate(matches):
        code = match.group(0)
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text_cell)
        name = text_cell[match.end() : end].strip(" -\t\n")
        name = " ".join(name.split())
        cuentas_meta.append((code, name))

    importes: List[float] = []
    for row in _iter_rows(df.iloc[start:]):
        if len(importes) >= len(cuentas_meta):
            break
        val = _parse_float(row[1] if len(row) > 1 else None)
        if val is None:
            continue
        importes.append(val)

    for idx, (code, name) in enumerate(cuentas_meta):
        importe = importes[idx] if idx < len(importes) else None
        cuentas.append(
            {
                "Cuenta_Codigo": code,
                "Cuenta_Nombre": name,
                "Cuenta_Tipo": "CAJA",
                "Cuenta_Importe": importe,
            }
        )

    info = (
        f"Demostracion del Saldo: {len(cuentas_meta)} codigos, {len(importes)} importes."
    )
    return cuentas, info


def _parse_table3_sitpat(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], str]:
    sitpat: List[Dict[str, Any]] = []
    info = ""

    col_c = df.iloc[:, 2] if df.shape[1] > 2 else pd.Series([], dtype=object)
    col_d = df.iloc[:, 3] if df.shape[1] > 3 else pd.Series([], dtype=object)

    text_c = ""
    text_d = ""
    for idx, val in col_c.items():
        if "estado de situacion patrimonial" in _normalize_key(val) or "activo" in _normalize_key(val):
            text_c = _normalize_text(val)
            text_d = _normalize_text(col_d.get(idx))
            break

    if not text_c:
        text_c = "\n".join([_normalize_text(v) for v in col_c if _normalize_text(v)])
        text_d = "\n".join([_normalize_text(v) for v in col_d if _normalize_text(v)])

    if not text_c:
        return sitpat, "No se encontro bloque de situacion patrimonial."

    lines_c = [line.strip() for line in text_c.splitlines() if line.strip()]
    lines_d = [line.strip() for line in text_d.splitlines() if line.strip()]

    items: List[Tuple[str, str]] = []
    for line in lines_c:
        low = line.lower()
        if low.startswith("estado de situacion patrimonial"):
            continue
        if low in {"activo", "pasivo", "patrimonio publico"}:
            continue
        if low.startswith("total"):
            continue
        match = _SITPAT_ITEM_RE.match(line)
        if match:
            items.append((match.group(1), match.group(2).strip()))

    amounts: List[float] = []
    for line in lines_d:
        val = _parse_float(line)
        if val is None:
            continue
        amounts.append(val)

    for idx, (code, name) in enumerate(items):
        saldo = amounts[idx] if idx < len(amounts) else None
        tipo = None
        if code.startswith("1"):
            tipo = "Activo"
        elif code.startswith("2"):
            tipo = "Pasivo + Patrimonio Publico"
        sitpat.append(
            {
                "SitPat_Codigo": code,
                "SitPat_Nombre": name,
                "SitPat_Tipo": tipo,
                "SitPat_Saldo": saldo,
                "SitPat_Observacion": None,
            }
        )

    info = f"Situacion patrimonial: {len(items)} items, {len(amounts)} saldos."
    return sitpat, info


def _parse_table4_metas(
    df: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[Tuple[str, str], int]]:
    metas: List[Dict[str, Any]] = []
    programas_extra: List[Dict[str, Any]] = []
    metas_por_programa: Dict[Tuple[str, str], int] = {}

    current_prog: Optional[Tuple[str, str, str]] = None

    for row in _iter_rows(df):
        row_texts = [_normalize_text(c) for c in row if _normalize_text(c)]
        if not row_texts:
            continue

        prog_match = None
        for text in row_texts:
            prog_match = _PROG_RE.search(text)
            if prog_match:
                break
        if prog_match:
            juri_code = prog_match.group(1)
            prog_code = prog_match.group(2)
            prog_name = prog_match.group(3).strip()
            current_prog = (juri_code, prog_code, prog_name)
            programas_extra.append(
                {
                    "Juri_Codigo": juri_code,
                    "Prog_Codigo": prog_code,
                    "Prog_Nombre": prog_name,
                    "Prog_Vigente": None,
                    "Prog_Preventivo": None,
                    "Prog_Compromiso": None,
                    "Prog_Devengado": None,
                    "Prog_Pagado": None,
                    "Prog_Tipo": "Presupuestarios",
                }
            )
            continue

        if current_prog is None:
            continue

        header_hit = any("programado" in t.lower() or "ejecutado" in t.lower() for t in row_texts)
        if header_hit:
            continue

        meta_text = None
        for text in row_texts:
            if _META_RE.match(text):
                meta_text = text
                break
        if not meta_text:
            continue

        meta_match = _META_RE.match(meta_text)
        if not meta_match:
            continue

        meta_code = meta_match.group(1)
        meta_rest = meta_match.group(2).strip()

        meta_unidad = None
        meta_nombre = meta_rest
        if "(" in meta_rest and meta_rest.endswith(")"):
            idx = meta_rest.rfind("(")
            meta_nombre = meta_rest[:idx].strip()
            meta_unidad = meta_rest[idx + 1 : -1].strip() or None

        nums = _row_numbers(row)
        if len(nums) < 3:
            logger.warning("Fila de meta incompleta: %s", meta_text)
            continue

        juri_code, prog_code, prog_name = current_prog
        metas.append(
            {
                "Juri_Codigo": juri_code,
                "Prog_Codigo": prog_code,
                "Prog_Nombre": prog_name,
                "Meta_Nombre": meta_nombre,
                "Meta_Unidad": meta_unidad,
                "Meta_Anual": nums[0],
                "Meta_Parcial": nums[1],
                "Meta_Ejecutado": nums[2],
                "Meta_Observacion": meta_code,
            }
        )

        key = (juri_code, prog_code)
        metas_por_programa[key] = metas_por_programa.get(key, 0) + 1

    return metas, programas_extra, metas_por_programa


def _ensure_ids(rows: List[Dict[str, Any]], doc_id: Any, muni_id: Any) -> None:
    for row in rows:
        row["ID_DocumentoCargado"] = doc_id
        row["ID_Municipio"] = muni_id


def _drop_keys(rows: List[Dict[str, Any]], keys: Iterable[str]) -> None:
    for row in rows:
        for key in keys:
            row.pop(key, None)


def ingest_rafam_xlsx(
    xlsx_path: str,
    id_documentoCargado: Any,
    id_municipio: Any,
    supabase_client,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Ingesta RAFAM desde XLSX (Table 1-4) y carga en Supabase.

    Si dry_run=True, no escribe en la base y devuelve dataframes/listas.
    """
    if not xlsx_path:
        raise ValueError("xlsx_path esta vacio.")
    if not id_documentoCargado:
        raise ValueError("id_documentoCargado esta vacio.")
    if not id_municipio:
        raise ValueError("id_municipio esta vacio.")

    sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, engine="openpyxl")
    missing = [name for name in [TABLE_1, TABLE_2, TABLE_3, TABLE_4] if name not in sheets]
    if missing:
        raise ValueError(f"Faltan hojas requeridas: {', '.join(missing)}")

    df1 = sheets[TABLE_1]
    df2 = sheets[TABLE_2]
    df3 = sheets[TABLE_3]
    df4 = sheets[TABLE_4]

    recursos = _parse_table1_recursos(df1)
    gastos = _parse_table1_gastos(df1)
    jurisdicciones, programas_base = _parse_table2_programas(df2)
    movimientos, mov_warnings = _parse_table3_movimientos(df3)
    cuentas, cuentas_info = _parse_table3_cuentas(df3)
    sitpat, sitpat_info = _parse_table3_sitpat(df3)
    metas, programas_extra, metas_por_programa = _parse_table4_metas(df4)

    periodo = _extract_periodo_from_table3(df3)
    for row in movimientos:
        row["MovTes_Periodo"] = periodo
        row["MovTes_Observacion"] = None

    if cuentas_info:
        logger.info(cuentas_info)
    if sitpat_info:
        logger.info(sitpat_info)
    for key, count in metas_por_programa.items():
        logger.info("Metas cargadas para programa %s-%s: %s", key[0], key[1], count)
    for warn in mov_warnings:
        logger.warning(warn)

    # merge programas y marcar metas
    prog_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for prog in programas_base + programas_extra:
        key = (prog.get("Juri_Codigo"), prog.get("Prog_Codigo"))
        if key not in prog_map:
            prog_map[key] = prog
    for key, prog in prog_map.items():
        prog["Prog_TieneMetas"] = key in metas_por_programa

    programas = list(prog_map.values())

    _ensure_ids(recursos, id_documentoCargado, id_municipio)
    _ensure_ids(gastos, id_documentoCargado, id_municipio)
    _ensure_ids(jurisdicciones, id_documentoCargado, id_municipio)
    _ensure_ids(movimientos, id_documentoCargado, id_municipio)
    _ensure_ids(cuentas, id_documentoCargado, id_municipio)
    _ensure_ids(sitpat, id_documentoCargado, id_municipio)

    if dry_run:
        return {
            "ok": True,
            "warnings": mov_warnings,
            "info": {
                "cuentas": cuentas_info,
                "sitpat": sitpat_info,
                "metas_por_programa": metas_por_programa,
            },
            "data": {
                "bd_recursos": pd.DataFrame(recursos),
                "bd_gastos": pd.DataFrame(gastos),
                "bd_jurisdiccion": pd.DataFrame(jurisdicciones),
                "bd_programas": pd.DataFrame(programas),
                "bd_movimientosTesoreria": pd.DataFrame(movimientos),
                "bd_cuentas": pd.DataFrame(cuentas),
                "bd_situacionpatrimonial": pd.DataFrame(sitpat),
                "bd_metas_raw": pd.DataFrame(metas),
            },
        }

    def _upsert_compat(table_name: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
        if not rows:
            return
        table = supabase_client.table(table_name)
        try:
            table.upsert(rows, on_conflict=on_conflict).execute()
        except TypeError:
            # SDK viejo: upsert sin on_conflict (usa constraint por defecto en Postgres)
            table.upsert(rows).execute()

    # Delete and insert by document
    delete_rows_by_filters(
        supabase_client, "bd_recursos", {"ID_DocumentoCargado": id_documentoCargado}
    )
    rec_inserted = insert_rows(supabase_client, "bd_recursos", recursos)

    delete_rows_by_filters(
        supabase_client, "bd_gastos", {"ID_DocumentoCargado": id_documentoCargado}
    )
    gasto_inserted = insert_rows(supabase_client, "bd_gastos", gastos)

    # Upsert jurisdicciones
    _upsert_compat(
        "bd_jurisdiccion",
        jurisdicciones,
        "ID_DocumentoCargado,Juri_Codigo",
    )

    # Mapear jurisdicciones a ID
    juri_rows = fetch_rows(
        supabase_client, "bd_jurisdiccion", {"ID_DocumentoCargado": id_documentoCargado}
    )
    juri_map: Dict[str, Any] = {
        row.get("Juri_Codigo"): row.get("ID_Jurisdiccion") for row in juri_rows
    }

    programas_db: List[Dict[str, Any]] = []
    for prog in programas:
        juri_code = prog.get("Juri_Codigo")
        juri_id = juri_map.get(juri_code)
        if not juri_id:
            logger.warning("Programa sin jurisdiccion: %s", prog.get("Prog_Nombre"))
            continue
        row = dict(prog)
        row["ID_Jurisdiccion"] = juri_id
        row.pop("Juri_Codigo", None)
        programas_db.append(row)

    # bd_programas no tiene ID_DocumentoCargado en el schema actual
    _drop_keys(programas_db, ["ID_DocumentoCargado", "ID_Municipio"])
    _upsert_compat(
        "bd_programas",
        programas_db,
        "ID_Jurisdiccion,Prog_Codigo",
    )

    # Mapear programas para metas
    prog_rows = []
    if juri_map:
        prog_rows = fetch_rows(
            supabase_client,
            "bd_programas",
            {"ID_Jurisdiccion": list(juri_map.values())},
        )
    prog_map_db: Dict[Tuple[str, str], Any] = {}
    for row in prog_rows:
        prog_code = str(row.get("Prog_Codigo") or "").strip()
        juri_id = row.get("ID_Jurisdiccion")
        if not prog_code or not juri_id:
            continue
        juri_code = None
        for code, jid in juri_map.items():
            if jid == juri_id:
                juri_code = code
                break
        if juri_code:
            prog_map_db[(juri_code, prog_code)] = row.get("ID_Programa")

    metas_db: List[Dict[str, Any]] = []
    for meta in metas:
        key = (str(meta.get("Juri_Codigo") or ""), str(meta.get("Prog_Codigo") or ""))
        prog_id = prog_map_db.get(key)
        if not prog_id:
            logger.warning("Meta sin programa: %s", meta.get("Meta_Nombre"))
            continue
        metas_db.append(
            {
                "ID_Programa": prog_id,
                "Meta_Nombre": meta.get("Meta_Nombre"),
                "Meta_Unidad": meta.get("Meta_Unidad"),
                "Meta_Anual": meta.get("Meta_Anual"),
                "Meta_Parcial": meta.get("Meta_Parcial"),
                "Meta_Ejecutado": meta.get("Meta_Ejecutado"),
                "Meta_Observacion": meta.get("Meta_Observacion"),
            }
        )

    if metas_db:
        delete_rows_by_filters(
            supabase_client,
            "bd_metas",
            {"ID_Programa": list({row["ID_Programa"] for row in metas_db})},
        )
    meta_inserted = insert_rows(supabase_client, "bd_metas", metas_db)

    delete_rows_by_filters(
        supabase_client,
        "bd_movimientosTesoreria",
        {"ID_DocumentoCargado": id_documentoCargado},
    )
    mov_inserted = insert_rows(
        supabase_client, "bd_movimientosTesoreria", movimientos
    )

    delete_rows_by_filters(
        supabase_client, "bd_cuentas", {"ID_DocumentoCargado": id_documentoCargado}
    )
    cuenta_inserted = insert_rows(supabase_client, "bd_cuentas", cuentas)

    delete_rows_by_filters(
        supabase_client,
        "bd_situacionpatrimonial",
        {"ID_DocumentoCargado": id_documentoCargado},
    )
    sit_inserted = insert_rows(supabase_client, "bd_situacionpatrimonial", sitpat)

    return {
        "ok": True,
        "warnings": mov_warnings,
        "info": {
            "cuentas": cuentas_info,
            "sitpat": sitpat_info,
            "metas_por_programa": metas_por_programa,
        },
        "metrics": {
            "recursos_rows_inserted": rec_inserted,
            "gastos_rows_inserted": gasto_inserted,
            "movimientos_rows_inserted": mov_inserted,
            "cuentas_rows_inserted": cuenta_inserted,
            "sitpat_rows_inserted": sit_inserted,
            "metas_rows_inserted": meta_inserted,
        },
    }
