# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import time
from typing import List, Optional
import httpx
from postgrest._sync.request_builder import SyncRequestBuilder
from supabase_client import get_supabase_client
from postgrest.exceptions import APIError
from pipeline.ingest_pdf import ingest_pdf
from pipeline.load_supabase import (
    fetch_documents,
    delete_rows,
    delete_pdf_from_storage,
    download_pdf_from_storage,
)
from pipeline.runner import run_document
from pipeline.ingest_xlsx import ingest_rafam_xlsx

# Compat: Streamlit >=1.25 uses st.rerun instead of st.experimental_rerun.
if not hasattr(st, "experimental_rerun") and hasattr(st, "rerun"):
    st.experimental_rerun = st.rerun

# -------------------------------------------------
# CONFIGURACIÓN BÁSICA
# -------------------------------------------------
st.set_page_config(page_title="Municipios PBA", layout="wide")
st.title("Municipios PBA – Navegador de información contable")

# Cliente de Supabase (usa tu supabase_client y secrets.toml con url/key)
supabase = get_supabase_client()

# -------------------------------------------------
# RETRIES + CACHE
# -------------------------------------------------
_orig_execute = SyncRequestBuilder.execute

def _execute_with_retry(self, *args, **kwargs):
    retries = 3
    base_sleep = 0.5
    last_exc = None
    for attempt in range(retries):
        try:
            return _orig_execute(self, *args, **kwargs)
        except (httpx.ReadError, httpx.ConnectError, httpx.ReadTimeout) as exc:
            last_exc = exc
            time.sleep(base_sleep * (2 ** attempt))
    raise last_exc

SyncRequestBuilder.execute = _execute_with_retry

@st.cache_data(ttl=300, show_spinner=False)
def _cached_select(table: str, filters: Optional[dict] = None):
    q = supabase.table(table).select("*")
    if filters:
        for k, v in filters.items():
            q = q.eq(k, v)
    res = q.execute()
    return res.data if res.data else []

# -------------------------------------------------
# HELPERS (sanitizar + guardar cambios)
# -------------------------------------------------
def _sanitize(v):
    # Convierte "" y NaN a None para evitar errores de Postgres (numeric/not-null)
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def guardar_cambios_df(
    *,
    tabla: str,
    pk_col: str,
    df_original: pd.DataFrame,
    df_editado: pd.DataFrame,
    columnas_editables: list,
):
    """
    Compara df_original vs df_editado y hace UPDATE fila por fila según pk_col.
    Devuelve cantidad de filas actualizadas.
    """
    if df_original is None or df_editado is None or df_original.empty:
        return 0

    if pk_col not in df_original.columns:
        st.error(
            f"No encuentro la PK '{pk_col}' en la tabla {tabla}. "
            f"Columnas disponibles: {list(df_original.columns)}"
        )
        return 0

    columnas_editables = [c for c in columnas_editables if c in df_original.columns]
    if not columnas_editables:
        st.warning("No hay columnas editables disponibles en el dataframe.")
        return 0

    rows_to_upsert = []
    for i in range(len(df_original)):
        orig = df_original.iloc[i]
        edit = df_editado.iloc[i]

        cambios = {}
        for col in columnas_editables:
            a = _sanitize(orig[col])
            b = _sanitize(edit[col])
            if a != b:
                cambios[col] = b

        if cambios:
            pk_val = orig[pk_col]
            cambios[pk_col] = pk_val
            rows_to_upsert.append(cambios)

    if rows_to_upsert:
        supabase.table(tabla).upsert(rows_to_upsert).execute()
        st.cache_data.clear()

    return len(rows_to_upsert)


def _make_editor(df: pd.DataFrame, columnas_editables: list, key: str):
    columnas_editables = [c for c in columnas_editables if c in df.columns]
    columnas_deshabilitadas = [c for c in df.columns if c not in columnas_editables]
    edited = st.data_editor(
        df,
        num_rows="fixed",
        disabled=columnas_deshabilitadas,
        key=key,
    )
    return edited, columnas_editables


def _delete_rows_ui(
    *,
    df: pd.DataFrame,
    pk_col: str,
    table_name: str,
    label: str,
    key_prefix: str,
    display_cols: Optional[List[str]] = None,
):
    if df is None or df.empty:
        st.info(f"No hay {label} para borrar.")
        return

    if pk_col not in df.columns:
        st.warning(f"No encontro la PK {pk_col} en {table_name}.")
        return

    cols = [pk_col]
    if display_cols:
        cols += [c for c in display_cols if c in df.columns and c != pk_col]

    df_del = df[cols].copy()
    df_del["Eliminar"] = False

    select_all = st.checkbox(
        f"Seleccionar todos los {label} visibles",
        key=f"{key_prefix}_delete_all",
    )
    if select_all:
        df_del["Eliminar"] = True

    edited = st.data_editor(
        df_del,
        num_rows="fixed",
        disabled=[c for c in df_del.columns if c != "Eliminar"],
        key=f"{key_prefix}_delete_editor",
    )

    confirm = st.checkbox(
        f"Confirmo borrar {label} seleccionados",
        key=f"{key_prefix}_delete_confirm",
    )
    if st.button(f"Borrar {label} seleccionados", key=f"{key_prefix}_delete_button"):
        if not confirm:
            st.error("Debes confirmar antes de borrar.")
            return
        ids = edited.loc[edited["Eliminar"] == True, pk_col].tolist()
        if not ids:
            st.warning("No seleccionaste registros para borrar.")
            return
        deleted = delete_rows(supabase, table_name, pk_col, ids)
        if deleted == 0:
            st.warning("No se borraron registros. Revisar permisos (RLS) o IDs.")
            return
        st.success(f"Registros borrados en {table_name}: {deleted}")
        st.experimental_rerun()


# -------------------------------------------------
# AUTENTICACIÓN
# -------------------------------------------------
# Inicializamos el estado del usuario si no existe
if "user" not in st.session_state:
    st.session_state["user"] = None


def login_form():
    st.header("Ingreso al sistema")

    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("Ingresar")

        if submitted:
            if not email or not password:
                st.error("Completá email y contraseña.")
                return

            try:
                # supabase-py 2.x: sign_in_with_password
                auth_response = supabase.auth.sign_in_with_password(
                    {"email": email, "password": password}
                )

                # Puede venir como objeto o como dict
                user = None
                if hasattr(auth_response, "user"):
                    user = auth_response.user
                elif isinstance(auth_response, dict):
                    user = auth_response.get("user")

                if not user:
                    st.error("No se pudo obtener el usuario desde Supabase.")
                    return

                st.session_state["user"] = user
                st.success("Inicio de sesión exitoso.")
                st.experimental_rerun()

            except Exception as e:
                st.error(f"Error al iniciar sesión: {e}")


def logout():
    try:
        supabase.auth.sign_out()
    except Exception:
        pass
    st.session_state["user"] = None
    st.experimental_rerun()


# Si no hay usuario autenticado, mostramos solo el login y frenamos
if st.session_state["user"] is None:
    login_form()
    st.stop()

# Si llega acá, hay usuario logueado
user = st.session_state["user"]
if isinstance(user, dict):
    email_usuario = user.get("email", "sin email")
else:
    email_usuario = getattr(user, "email", "sin email")

st.sidebar.write(f"Usuario: {email_usuario}")
if st.sidebar.button("Cerrar sesión"):
    logout()

# -------------------------------------------------
# 1) LISTADO Y SELECCIÓN DE MUNICIPIO
# -------------------------------------------------
municipios = _cached_select("bd_municipios")

if not municipios:
    st.info("Todavía no hay municipios cargados en la tabla bd_municipios.")
    st.stop()

st.subheader("Listado de municipios (vista rápida)")

columnas_vista = [
    "Muni_Nombre",
    "Muni_SeccionElectoral",
    "Muni_Poblacion_2022",
    "Muni_Superficie",
    "Muni_Densidad",
    "Muni_Categoria",
]
columnas_vista = [c for c in columnas_vista if c in municipios[0].keys()]

st.dataframe([{c: m.get(c) for c in columnas_vista} for m in municipios])

st.subheader("Seleccionar municipio")

opciones_muni = {
    f"{m['Muni_Nombre']} (Sec. {m.get('Muni_SeccionElectoral', 's/d')})": m["ID_Municipio"]
    for m in municipios
}

opciones_muni_ordenadas = sorted(opciones_muni.keys())
nombre_muni_sel = st.selectbox("Eleg?� un municipio", opciones_muni_ordenadas)

if not nombre_muni_sel:
    st.stop()

id_muni_sel = opciones_muni[nombre_muni_sel]

# Si cambia el municipio, reseteamos el documento seleccionado
prev_muni_id = st.session_state.get("municipio_seleccionado_id")
if prev_muni_id is not None and prev_muni_id != id_muni_sel:
    st.session_state.pop("documento_seleccionado_id", None)
    st.session_state.pop("documento_seleccionado_nombre", None)

st.session_state["municipio_seleccionado_id"] = id_muni_sel
st.session_state["municipio_seleccionado_nombre"] = nombre_muni_sel

st.success(f"Municipio seleccionado: {nombre_muni_sel}")

# -------------------------------------------------
# 2) FICHA DEL MUNICIPIO
# -------------------------------------------------
st.markdown("---")
st.subheader("Ficha del municipio")

res_m = (
    supabase.table("bd_municipios")
    .select("*")
    .eq("ID_Municipio", id_muni_sel)
    .single()
    .execute()
)
muni = res_m.data

if muni is None:
    st.error("No se encontró el municipio en la base.")
    st.stop()

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"**Nombre:** {muni.get('Muni_Nombre', 's/d')}")
    st.markdown(f"**Sección electoral:** {muni.get('Muni_SeccionElectoral', 's/d')}")
    st.markdown(f"**Ciudad cabecera:** {muni.get('Muni_CiudadCabecera', 's/d')}")
    st.markdown(f"**Año de creación:** {muni.get('Muni_AnoCreacion', 's/d')}")

with col2:
    st.markdown(f"**Población 2022:** {muni.get('Muni_Poblacion_2022', 's/d')}")
    st.markdown(f"**Superficie (km²):** {muni.get('Muni_Superficie', 's/d')}")
    st.markdown(f"**Densidad (hab/km²):** {muni.get('Muni_Densidad', 's/d')}")
    st.markdown(f"**Categoría:** {muni.get('Muni_Categoria', 's/d')}")

with col3:
    st.markdown(f"**Trabajadores:** {muni.get('Muni_Cantidad_Trabajadores', 's/d')}")
    st.markdown(f"**Concejales:** {muni.get('Muni_Cantidad_Concejales', 's/d')}")
    st.markdown(
        f"**Consejeros escolares:** {muni.get('Muni_Cantidad_ConsejerosEscolares', 's/d')}"
    )
    st.markdown(f"**Intendente actual:** {muni.get('Muni_IntendenteActual', 's/d')}")

st.markdown("### Links disponibles")

cols_links = st.columns(3)
with cols_links[0]:
    link_bo = muni.get("Muni_LinkBO")
    if link_bo:
        st.markdown(f"[Boletín Oficial]({link_bo})")
    else:
        st.caption("Boletín Oficial: sin link")

with cols_links[1]:
    link_doc = muni.get("Muni_LinkDocumentoContableEncontrado")
    if link_doc:
        st.markdown(f"[Doc. contable]({link_doc})")
    else:
        st.caption("Documento contable: sin link")

with cols_links[2]:
    link_datos = muni.get("Muni_LinkSectorDatos")
    if link_datos:
        st.markdown(f"[Sector de datos]({link_datos})")
    else:
        st.caption("Sector de datos: sin link")

# -------------------------------------------------
# 3) DOCUMENTOS DEL MUNICIPIO (RENDICIONES / PRESUPUESTOS)
# -------------------------------------------------
st.markdown("---")
st.subheader("Documentos cargados para este municipio")

documentos = _cached_select("BD_DocumentosCargados", {"ID_Municipio": id_muni_sel})

doc_id_sel = st.session_state.get("documento_seleccionado_id", None)

if documentos:
    opciones_docs = {
        f'{d.get("Doc_Nombre", "")} ({d.get("Doc_Tipo", "")} {d.get("Doc_Periodo", "")} {d.get("Doc_Anio", "")})': d[
            "ID_DocumentoCargado"
        ]
        for d in documentos
    }

    nombres_docs = list(opciones_docs.keys())
    index_default = 0
    if doc_id_sel:
        for i, nombre in enumerate(nombres_docs):
            if opciones_docs[nombre] == doc_id_sel:
                index_default = i
                break

    nombre_doc_sel = st.selectbox(
        "Elegí un documento", nombres_docs, index=index_default
    )
    doc_id_sel = opciones_docs[nombre_doc_sel]
    st.session_state["documento_seleccionado_id"] = doc_id_sel
    st.session_state["documento_seleccionado_nombre"] = nombre_doc_sel

    st.success(f"Documento seleccionado: {nombre_doc_sel}")
else:
    st.info("Este municipio todavía no tiene documentos cargados.")

st.markdown("#### Eliminar documento seleccionado")
if doc_id_sel:
    doc_sel = next(
        (d for d in documentos if d.get("ID_DocumentoCargado") == doc_id_sel), None
    )
    confirm_del_doc = st.checkbox(
        "Confirmo borrar el documento y su PDF",
        key="confirm_delete_doc",
    )
    if st.button("Borrar documento seleccionado", key="delete_doc_button"):
        if not confirm_del_doc:
            st.error("Debes confirmar antes de borrar.")
        else:
            if doc_sel and doc_sel.get("Doc_ArchivoStoragePath"):
                try:
                    delete_pdf_from_storage(
                        supabase, "pdfs", doc_sel["Doc_ArchivoStoragePath"]
                    )
                except Exception as e:
                    st.error(f"Error al borrar PDF: {e}")
            deleted = delete_rows(
                supabase, "BD_DocumentosCargados", "ID_DocumentoCargado", [doc_id_sel]
            )
            st.success(f"Documento borrado: {deleted}")
            st.experimental_rerun()
else:
    st.info("No hay documento seleccionado para borrar.")

st.markdown("#### Crear nuevo documento")

with st.form("form_documento"):
    doc_tipo = st.selectbox("Tipo de documento", ["Rendicion", "Presupuesto"])
    doc_periodo = st.selectbox("Periodo", ["Q1", "Q2", "Q3", "Q4", "Anual"])
    doc_anio = st.number_input("Ano", min_value=2000, max_value=2100, value=2025)
    doc_nombre_input = st.text_input("Nombre del documento (opcional)", "")
    doc_pdf = st.file_uploader("PDF del documento", type=["pdf"])

    submitted_doc = st.form_submit_button("Crear documento")

    if submitted_doc:
        if doc_pdf is None:
            st.error("Debes subir un PDF para crear el documento.")
            st.stop()

        doc_nombre_final = (
            doc_nombre_input
            if doc_nombre_input.strip()
            else f"{doc_tipo} {doc_periodo} {int(doc_anio)}"
        )

        metadata = {
            "ID_Municipio": id_muni_sel,
            "Doc_Tipo": doc_tipo,
            "Doc_Periodo": doc_periodo,
            "Doc_Anio": int(doc_anio),
            "Doc_Nombre": doc_nombre_final,
        }

        pdf_bytes = doc_pdf.getvalue()
        result = ingest_pdf(supabase, pdf_bytes, doc_pdf.name, metadata)
        if result["ok"]:
            st.success("Documento creado y PDF subido correctamente.")
            st.experimental_rerun()
        else:
            st.error(f"Error al crear el documento: {result['error']}")

# -------------------------------------------------
# 3.1) PROCESAR DOCUMENTO (MANUAL)
# -------------------------------------------------
st.markdown("---")
st.subheader("Procesar documento (manual)")

try:
    docs_pendientes = fetch_documents(
        supabase,
        {"ID_Municipio": id_muni_sel, "Doc_Estado": "Pendiente"},
    )
except Exception as e:
    st.error(f"Error al buscar documentos pendientes: {e}")
    docs_pendientes = []

docs_pendientes_con_pdf = [
    d for d in docs_pendientes if d.get("Doc_ArchivoStoragePath")
]
docs_pendientes_sin_pdf = [
    d for d in docs_pendientes if not d.get("Doc_ArchivoStoragePath")
]

if not docs_pendientes_con_pdf:
    if docs_pendientes_sin_pdf:
        st.info("Este documento no tiene PDF cargado. Subilo primero.")
    else:
        st.info("No hay documentos pendientes para procesar.")
else:
    opciones_pendientes = {
        f'{d.get("Doc_Nombre", "s/n")} ({d.get("Doc_Tipo", "")} {d.get("Doc_Periodo", "")} {d.get("Doc_Anio", "")})': d.get(
            "ID_DocumentoCargado"
        )
        for d in docs_pendientes_con_pdf
    }

    nombre_doc_pend = st.selectbox(
        "Documento pendiente", list(opciones_pendientes.keys())
    )
    doc_id_pend = opciones_pendientes.get(nombre_doc_pend)

    if st.button("Procesar ahora"):
        progress = st.progress(0, text="Iniciando proceso...")
        progress.progress(25, text="Ejecutando runner...")
        resultado = run_document(supabase, doc_id_pend)
        progress.progress(100, text="Finalizado.")

        if resultado["ok"]:
            st.success("Documento procesado correctamente.")
            if resultado.get("metrics"):
                st.write("Metricas:", resultado["metrics"])
        else:
            st.error(f"Error al procesar: {resultado['error']}")

# -------------------------------------------------
# 3.1.1) PROCESAR DOCUMENTO (XLSX RAFAM)
# -------------------------------------------------
st.markdown("---")
st.subheader("Procesar documento (XLSX RAFAM)")
st.caption("Carga desde Excel tabulado (Table 1-4).")

if documentos:
    opciones_docs_xlsx = {
        f'{d.get("Doc_Nombre", "s/n")} ({d.get("Doc_Tipo", "")} {d.get("Doc_Periodo", "")} {d.get("Doc_Anio", "")})': d.get(
            "ID_DocumentoCargado"
        )
        for d in documentos
    }

    nombres_xlsx = list(opciones_docs_xlsx.keys())
    index_xlsx = 0
    if doc_id_sel:
        for i, nombre in enumerate(nombres_xlsx):
            if opciones_docs_xlsx[nombre] == doc_id_sel:
                index_xlsx = i
                break

    nombre_doc_xlsx = st.selectbox(
        "Documento destino (ID_DocumentoCargado)",
        nombres_xlsx,
        index=index_xlsx,
        key="xlsx_doc_sel",
    )
    doc_id_xlsx = opciones_docs_xlsx[nombre_doc_xlsx]

    xlsx_file = st.file_uploader("XLSX RAFAM", type=["xlsx"], key="xlsx_file")
    xlsx_path_input = st.text_input("Ruta local al XLSX (opcional)", key="xlsx_path")
    dry_run_xlsx = st.checkbox("Dry run (no escribe en DB)", key="xlsx_dry_run")

    if st.button("Procesar XLSX", key="xlsx_run"):
        xlsx_path = ""
        if xlsx_file is not None:
            os.makedirs("logs", exist_ok=True)
            xlsx_path = os.path.join(
                "logs",
                f"rafam_xlsx_{doc_id_xlsx}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx",
            )
            with open(xlsx_path, "wb") as handle:
                handle.write(xlsx_file.getvalue())
        else:
            xlsx_path = (xlsx_path_input or "").strip()
            if xlsx_path and not os.path.exists(xlsx_path):
                st.error("Ruta de XLSX invalida o no existe.")
                xlsx_path = ""

        if not xlsx_path:
            st.error("Debes subir un XLSX o indicar una ruta local valida.")
            st.stop()

        try:
            with st.spinner("Procesando XLSX..."):
                resultado = ingest_rafam_xlsx(
                    xlsx_path,
                    doc_id_xlsx,
                    id_muni_sel,
                    supabase,
                    dry_run=dry_run_xlsx,
                )
        except Exception as exc:
            st.error(f"Error al procesar XLSX: {exc}")
            st.stop()

        if resultado.get("ok"):
            st.success("XLSX procesado correctamente.")
            if resultado.get("warnings"):
                st.write("Warnings:", resultado["warnings"])
            if resultado.get("info"):
                st.write("Info:", resultado["info"])
            if resultado.get("metrics"):
                st.write("Metricas:", resultado["metrics"])
            if dry_run_xlsx and resultado.get("data"):
                st.markdown("### Preview (dry run)")
                for name, df in resultado["data"].items():
                    st.markdown(f"**{name}** - filas: {len(df)}")
                    st.dataframe(df.head(10))
        else:
            st.error(f"Error al procesar XLSX: {resultado.get('error')}")
else:
    st.info("No hay documentos disponibles para procesar XLSX.")

# -------------------------------------------------
# 3.1.2) PROCESAR DOCUMENTO (XLSX LLM)
# -------------------------------------------------
st.markdown("---")
st.subheader("Procesar documento (XLSX + LLM)")
st.caption("Pipeline LLM para XLSX (distinto del Single-Shot PDF).")

if documentos:
    opciones_docs_xlsx_llm = {
        f'{d.get("Doc_Nombre", "s/n")} ({d.get("Doc_Tipo", "")} {d.get("Doc_Periodo", "")} {d.get("Doc_Anio", "")})': d.get(
            "ID_DocumentoCargado"
        )
        for d in documentos
    }

    nombres_xlsx_llm = list(opciones_docs_xlsx_llm.keys())
    index_xlsx_llm = 0
    if doc_id_sel:
        for i, nombre in enumerate(nombres_xlsx_llm):
            if opciones_docs_xlsx_llm[nombre] == doc_id_sel:
                index_xlsx_llm = i
                break

    nombre_doc_xlsx_llm = st.selectbox(
        "Documento destino (ID_DocumentoCargado)",
        nombres_xlsx_llm,
        index=index_xlsx_llm,
        key="xlsx_llm_doc_sel",
    )
    doc_id_xlsx_llm = opciones_docs_xlsx_llm[nombre_doc_xlsx_llm]

    xlsx_file_llm = st.file_uploader("XLSX RAFAM", type=["xlsx"], key="xlsx_llm_file")
    xlsx_path_input_llm = st.text_input(
        "Ruta local al XLSX (opcional)", key="xlsx_llm_path"
    )

    if st.button("Procesar XLSX con LLM", key="xlsx_llm_run"):
        xlsx_path_llm = ""
        if xlsx_file_llm is not None:
            os.makedirs("logs", exist_ok=True)
            xlsx_path_llm = os.path.join(
                "logs",
                f"xlsx_llm_{doc_id_xlsx_llm}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx",
            )
            with open(xlsx_path_llm, "wb") as handle:
                handle.write(xlsx_file_llm.getvalue())
        else:
            xlsx_path_llm = (xlsx_path_input_llm or "").strip()
            if xlsx_path_llm and not os.path.exists(xlsx_path_llm):
                st.error("Ruta de XLSX invalida o no existe.")
                xlsx_path_llm = ""

        if not xlsx_path_llm:
            st.error("Debes subir un XLSX o indicar una ruta local valida.")
            st.stop()

        openai_key = ""
        try:
            openai_key = st.secrets.get("openai", {}).get("api_key", "")
        except Exception:
            openai_key = ""
        if not openai_key:
            openai_key = os.getenv("OPENAI_API_KEY", "")
        if not openai_key:
            st.error("Falta OPENAI_API_KEY (env o st.secrets).")
        else:
            try:
                from openai import OpenAI
                from single_shot.pipeline_xlsx import run_single_shot_xlsx
                from single_shot.settings import load_settings
            except Exception as exc:
                st.error(f"No se pudo cargar el pipeline XLSX LLM: {exc}")
            else:
                with st.spinner("Ejecutando XLSX + LLM..."):
                    settings = load_settings()
                    client_openai = OpenAI(api_key=openai_key)
                    log_path = os.path.join(
                        "logs",
                        f"xlsx_llm_{time.strftime('%Y%m%d_%H%M%S')}.jsonl",
                    )
                    summary = run_single_shot_xlsx(
                        client_openai=client_openai,
                        client_supabase=supabase,
                        xlsx_path=xlsx_path_llm,
                        id_municipio=id_muni_sel,
                        log_path=log_path,
                        model=settings.openai_model,
                        max_retries=settings.max_retries,
                        retry_sleep_sec=settings.retry_sleep_sec,
                        metas_staging_table=settings.metas_staging_table,
                        doc_id=doc_id_xlsx_llm,
                    )

                st.success("XLSX + LLM finalizado.")
                st.json(summary)
                st.caption(f"Log: {log_path}")
else:
    st.info("No hay documentos disponibles para procesar XLSX + LLM.")

# -------------------------------------------------
# -------------------------------------------------
# 3.2) PROCESAR DOCUMENTO (SINGLE-SHOT)
# -------------------------------------------------
st.markdown("---")
st.subheader("Procesar documento (Single-Shot)")
st.caption("Una sola llamada LLM para cargar todas las tablas en bd_*.")

if documentos:
    documentos_ss = [d for d in documentos if d.get("ID_Municipio") == id_muni_sel]
    if not documentos_ss:
        st.info("No hay documentos del municipio seleccionado para Single-Shot.")
    else:
        opciones_docs_ss = {
            f'{d.get("Doc_Nombre", "s/n")} ({d.get("Doc_Tipo", "")} {d.get("Doc_Periodo", "")} {d.get("Doc_Anio", "")})': d.get(
                "ID_DocumentoCargado"
            )
            for d in documentos_ss
        }

        nombres_ss = list(opciones_docs_ss.keys())
        index_ss = 0
        if doc_id_sel:
            for i, nombre in enumerate(nombres_ss):
                if opciones_docs_ss[nombre] == doc_id_sel:
                    index_ss = i
                    break

        nombre_doc_ss = st.selectbox(
            "Documento destino (ID_DocumentoCargado)",
            nombres_ss,
            index=index_ss,
            key="ss_doc_sel",
        )
        doc_id_ss = opciones_docs_ss[nombre_doc_ss]
        pdf_path_ss = st.text_input("Ruta local al PDF (opcional)", key="ss_pdf_path")

        if st.button("Procesar con Single-Shot", key="ss_run"):
            pdf_source_path = (pdf_path_ss or "").strip()
            if pdf_source_path:
                if not os.path.exists(pdf_source_path):
                    st.error("Ruta de PDF invalida o no existe.")
                    pdf_source_path = ""
            if not pdf_source_path:
                doc_sel = next((d for d in documentos_ss if d.get("ID_DocumentoCargado") == doc_id_ss), None)
                storage_path = doc_sel.get("Doc_ArchivoStoragePath") if doc_sel else None
                if not storage_path:
                    st.error("No hay PDF en storage para este documento. Carga una ruta local.")
                    pdf_source_path = ""
                else:
                    try:
                        pdf_bytes = download_pdf_from_storage(supabase, "pdfs", storage_path)
                        os.makedirs("logs", exist_ok=True)
                        pdf_source_path = os.path.join(
                            "logs",
                            f"single_shot_pdf_{doc_id_ss}_{time.strftime('%Y%m%d_%H%M%S')}.pdf",
                        )
                        with open(pdf_source_path, "wb") as handle:
                            handle.write(pdf_bytes)
                    except Exception as exc:
                        st.error(f"No se pudo descargar el PDF desde storage: {exc}")
                        pdf_source_path = ""
            if not pdf_source_path:
                st.stop()

            openai_key = ""
            try:
                openai_key = st.secrets.get("openai", {}).get("api_key", "")
            except Exception:
                openai_key = ""
            if not openai_key:
                openai_key = os.getenv("OPENAI_API_KEY", "")
            if not openai_key:
                st.error("Falta OPENAI_API_KEY (env o st.secrets).")
            else:
                try:
                    from openai import OpenAI
                    from single_shot.pipeline import run_single_shot
                    from single_shot.settings import load_settings
                except Exception as exc:
                    st.error(f"No se pudo cargar el pipeline Single-Shot: {exc}")
                else:
                    with st.spinner("Ejecutando Single-Shot..."):
                        settings = load_settings()
                        client_openai = OpenAI(api_key=openai_key)
                        log_path = os.path.join(
                            "logs",
                            f"single_shot_{time.strftime('%Y%m%d_%H%M%S')}.jsonl",
                        )
                        summary = run_single_shot(
                            client_openai=client_openai,
                            client_supabase=supabase,
                            pdf_path=pdf_source_path,
                            id_municipio=id_muni_sel,
                            log_path=log_path,
                            model=settings.openai_model,
                            max_retries=settings.max_retries,
                            retry_sleep_sec=settings.retry_sleep_sec,
                            metas_staging_table=settings.metas_staging_table,
                            doc_id=doc_id_ss,
                        )

                    st.success("Single-Shot finalizado.")
                    st.json(summary)
                    st.caption(f"Log: {log_path}")
else:
    st.info("No hay documentos disponibles para ejecutar Single-Shot.")

# Si todavía no hay documento seleccionado, frenamos antes de las pestañas
doc_id_sel = st.session_state.get("documento_seleccionado_id", None)

if not doc_id_sel:
    st.warning("Seleccioná un documento (o creá uno nuevo) para cargar información.")
    st.stop()

# -------------------------------------------------
# 4) AVANCE DE CARGA POR DOCUMENTO
# -------------------------------------------------
st.markdown("---")
st.subheader("Avance de carga del documento")


def hay_registros(tabla: str) -> bool:
    # Tablas que tienen ID_DocumentoCargado directamente
    if tabla in [
        "bd_recursos",
        "bd_gastos",
        "bd_jurisdiccion",
        "bd_situacionpatrimonial",
        "bd_movimientosTesoreria",
        "bd_cuentas",
    ]:
        res = (
            supabase.table(tabla)
            .select("ID_DocumentoCargado")
            .eq("ID_DocumentoCargado", doc_id_sel)
            .limit(1)
            .execute()
        )
        return bool(res.data)

    # Programas: dependen de las jurisdicciones del documento
    if tabla == "bd_programas":
        res_j = (
            supabase.table("bd_jurisdiccion")
            .select("ID_Jurisdiccion")
            .eq("ID_DocumentoCargado", doc_id_sel)
            .execute()
        )
        jurisdicciones = res_j.data or []
        if not jurisdicciones:
            return False

        juri_ids = [j["ID_Jurisdiccion"] for j in jurisdicciones]

        res_p = (
            supabase.table("bd_programas")
            .select("ID_Programa")
            .in_("ID_Jurisdiccion", juri_ids)
            .limit(1)
            .execute()
        )
        return bool(res_p.data)

    # Metas: dependen de los programas del documento
    if tabla == "bd_metas":
        # Jurisdicciones del documento
        res_j = (
            supabase.table("bd_jurisdiccion")
            .select("ID_Jurisdiccion")
            .eq("ID_DocumentoCargado", doc_id_sel)
            .execute()
        )
        jurisdicciones = res_j.data or []
        if not jurisdicciones:
            return False

        juri_ids = [j["ID_Jurisdiccion"] for j in jurisdicciones]

        # Programas de esas jurisdicciones
        res_p = (
            supabase.table("bd_programas")
            .select("ID_Programa")
            .in_("ID_Jurisdiccion", juri_ids)
            .execute()
        )
        programas = res_p.data or []
        if not programas:
            return False

        prog_ids = [p["ID_Programa"] for p in programas]

        # Metas de esos programas
        res_m = (
            supabase.table("bd_metas")
            .select("ID_Meta")
            .in_("ID_Programa", prog_ids)
            .limit(1)
            .execute()
        )
        return bool(res_m.data)

    return False


estado = {
    "Recursos": hay_registros("bd_recursos"),
    "Gastos": hay_registros("bd_gastos"),
    "Jurisdicciones": hay_registros("bd_jurisdiccion"),
    "Programas": hay_registros("bd_programas"),
    "Situación Patrimonial": hay_registros("bd_situacionpatrimonial"),
    "Tesorería": hay_registros("bd_movimientosTesoreria"),
    "Cuentas": hay_registros("bd_cuentas"),
    "Metas": hay_registros("bd_metas"),
}

cols_estado = st.columns(4)
i = 0
for nombre, ok in estado.items():
    with cols_estado[i % 4]:
        st.write(f"**{nombre}**")
        st.write("Cargada" if ok else "Pendiente")
    i += 1

# -------------------------------------------------
# 5) PESTAÑAS PRINCIPALES
# -------------------------------------------------
tab_recursos, tab_gastos, tab_jurisdicciones, tab_programas, tab_sitpat, tab_tesoreria, tab_cuentas, tab_metas = st.tabs(
    [
        "Recursos",
        "Gastos",
        "Jurisdicciones",
        "Programas",
        "Situación Patrimonial",
        "Tesorería",
        "Cuentas",
        "Metas",
    ]
)

# ---------------- TAB RECURSOS ----------------
with tab_recursos:
    st.subheader("Recursos del documento")

    res_r = (
        supabase.table("bd_recursos")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .eq("ID_Municipio", id_muni_sel)  # filtro extra por municipio
        .execute()
    )
    recursos = res_r.data if res_r.data else []
    df_rec = pd.DataFrame(recursos) if recursos else pd.DataFrame()

    if recursos:
        df_rec = pd.DataFrame(recursos)

        # Aseguramos que existan las columnas esperadas
        for col in ["Rec_Tipo", "Rec_Vigente", "Rec_Devengado", "Rec_Percibido"]:
            if col not in df_rec.columns:
                df_rec[col] = None

        # Totales solo para Presupuestario + Extrapresupuestario
        mask_totales = df_rec["Rec_Tipo"].isin(["Presupuestario", "Extrapresupuestario"])
        df_totales = df_rec[mask_totales].copy()

        nombres_origen = [
            "recursos de capital",
            "ingresos corrientes",
            "fuentes financieras",
            "de libre disponibilidad",
            "afectados",
        ]
        rec_nombre = df_rec["Rec_Nombre"].fillna("").astype(str).str.lower()
        mask_origen = rec_nombre.apply(
            lambda v: any(nombre in v for nombre in nombres_origen)
        )
        mask_extra = rec_nombre.str.contains("extrapresupuestario")

        suma_vigente_origen = df_rec.loc[mask_origen, "Rec_Vigente"].fillna(0).sum()
        suma_vigente_extra = df_rec.loc[mask_extra, "Rec_Vigente"].fillna(0).sum()
        total_vigente = (suma_vigente_origen / 2) + suma_vigente_extra

        suma_devengado_origen = df_rec.loc[mask_origen, "Rec_Devengado"].fillna(0).sum()
        suma_devengado_extra = df_rec.loc[mask_extra, "Rec_Devengado"].fillna(0).sum()
        total_devengado = (suma_devengado_origen / 2) + suma_devengado_extra

        suma_origen = df_rec.loc[mask_origen, "Rec_Percibido"].fillna(0).sum()
        suma_extra = df_rec.loc[mask_extra, "Rec_Percibido"].fillna(0).sum()
        total_percibido = (suma_origen / 2) + suma_extra

        st.markdown("### Totales del documento (Presupuestario + Extrapresupuestario)")
        col_tot1, col_tot2, col_tot3 = st.columns(3)
        with col_tot1:
            st.metric("Total Vigente", f"{total_vigente:,.2f}")
        with col_tot2:
            st.metric("Total Devengado", f"{total_devengado:,.2f}")
        with col_tot3:
            st.metric("Total Percibido", f"{total_percibido:,.2f}")

        st.markdown("### Editar recursos existentes")

        columnas_editables = [
            "Rec_Nombre",
            "Rec_Tipo",
            "Rec_Categoria",
            "Rec_Vigente",
            "Rec_Devengado",
            "Rec_Percibido",
            "Rec_Observacion",
        ]

        edited_df = None
        try:
            edited_df, columnas_editables_ok = _make_editor(
                df_rec, columnas_editables, key="editor_recursos"
            )
        except Exception:
            st.dataframe(df_rec)
            columnas_editables_ok = [c for c in columnas_editables if c in df_rec.columns]

        if st.button("Guardar cambios en recursos"):
            # Ajustar si tu PK tiene otro nombre
            pk_recursos = "ID_Recurso"
            updates = guardar_cambios_df(
                tabla="bd_recursos",
                pk_col=pk_recursos,
                df_original=df_rec,
                df_editado=edited_df if edited_df is not None else df_rec,
                columnas_editables=columnas_editables_ok,
            )
            st.success(f"Cambios guardados en recursos. Registros actualizados: {updates}")
            st.experimental_rerun()

    else:
        st.info("No hay recursos cargados todavía para este documento.")

    st.markdown("### Borrar recursos")
    _delete_rows_ui(
        df=df_rec,
        pk_col="ID_Recurso",
        table_name="bd_recursos",
        label="recursos",
        key_prefix="recursos",
        display_cols=["Rec_Nombre", "Rec_Tipo", "Rec_Categoria"],
    )

    st.markdown("### Agregar nuevo recurso")
    with st.form("form_recurso"):
        rec_nombre = st.text_input("Nombre del recurso (ej. Ingresos Corrientes)")

        rec_tipo = st.selectbox(
            "Tipo de recurso",
            [
                "Presupuestario",
                "Extrapresupuestario",
            ],
        )

        rec_categoria = st.selectbox(
            "Categoría del recurso",
            [
                "Ingresos Corrientes",
                "Recursos de Capital",
                "Fuentes Financieras",
                "De Libre Disponibilidad",
                "Afectados",
                "Extrapresupuestarios",
            ],
        )

        rec_vigente = st.number_input("Vigente", min_value=0.0, step=1.0)
        rec_devengado = st.number_input("Devengado", min_value=0.0, step=1.0)
        rec_percibido = st.number_input("Percibido", min_value=0.0, step=1.0)
        rec_obs = st.text_area("Observaciones")

        submitted_rec = st.form_submit_button("Guardar recurso")

        if submitted_rec:
            if not rec_nombre:
                st.error("El nombre del recurso es obligatorio.")
            else:
                dato = {
                    "ID_DocumentoCargado": doc_id_sel,
                    "ID_Municipio": id_muni_sel,
                    "Rec_Nombre": rec_nombre,
                    "Rec_Tipo": rec_tipo,
                    "Rec_Categoria": rec_categoria,
                    "Rec_Vigente": rec_vigente,
                    "Rec_Devengado": rec_devengado,
                    "Rec_Percibido": rec_percibido,
                    "Rec_Observacion": _sanitize(rec_obs),
                }
                supabase.table("bd_recursos").insert(dato).execute()
                st.success("Recurso guardado correctamente.")
                st.experimental_rerun()

# ---------------- TAB GASTOS ----------------
with tab_gastos:
    st.subheader("Gastos del documento")

    res_g = (
        supabase.table("bd_gastos")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .execute()
    )
    gastos = res_g.data if res_g.data else []
    df_g = pd.DataFrame(gastos) if gastos else pd.DataFrame()

    # -------------------------------------------------
    # EDITAR EXISTENTES + TOTALES
    # -------------------------------------------------
    if gastos:
        df_g = pd.DataFrame(gastos)

        # Asegurar columnas numéricas
        cols_gasto_numericas = [
            "Gasto_Vigente",
            "Gasto_Preventivo",
            "Gasto_Compromiso",
            "Gasto_Devengado",
            "Gasto_Pagado",
        ]
        for col in cols_gasto_numericas:
            if col not in df_g.columns:
                df_g[col] = 0.0

        # Asegurar columna categoría (por si hay registros viejos sin el campo)
        if "Gasto_Categoria" not in df_g.columns:
            df_g["Gasto_Categoria"] = "Presupuestarios"  # default

        total_gasto_vigente = df_g["Gasto_Vigente"].fillna(0).sum()
        total_gasto_preventivo = df_g["Gasto_Preventivo"].fillna(0).sum()
        total_gasto_compromiso = df_g["Gasto_Compromiso"].fillna(0).sum()
        total_gasto_devengado = df_g["Gasto_Devengado"].fillna(0).sum()
        total_gasto_pagado = df_g["Gasto_Pagado"].fillna(0).sum()

        st.markdown("### Totales del documento (Gastos)")
        col_g1, col_g2, col_g3, col_g4, col_g5 = st.columns(5)
        with col_g1:
            st.metric("Vigente", f"{total_gasto_vigente:,.2f}")
        with col_g2:
            st.metric("Preventivo", f"{total_gasto_preventivo:,.2f}")
        with col_g3:
            st.metric("Compromiso", f"{total_gasto_compromiso:,.2f}")
        with col_g4:
            st.metric("Devengado", f"{total_gasto_devengado:,.2f}")
        with col_g5:
            st.metric("Pagado", f"{total_gasto_pagado:,.2f}")

        st.markdown("### Editar gastos existentes")

        columnas_editables_g = [
            "Gasto_Categoria",   # <-- NUEVA
            "Gasto_Objeto",
            "Gasto_Vigente",
            "Gasto_Preventivo",
            "Gasto_Compromiso",
            "Gasto_Devengado",
            "Gasto_Pagado",
            "Gasto_Observacion",
        ]

        edited_df_g, columnas_editables_g_ok = _make_editor(
            df_g, columnas_editables_g, key="editor_gastos"
        )

        # (Opcional, pero recomendado) Normalizar valores por si el usuario escribe cualquier cosa
        if "Gasto_Categoria" in edited_df_g.columns:
            edited_df_g["Gasto_Categoria"] = (
                edited_df_g["Gasto_Categoria"]
                .fillna("Presupuestarios")
                .astype(str)
                .replace(
                    {
                        "Presupuestario": "Presupuestarios",
                        "Extrapresupuestario": "Extrapresupuestarios",
                        "Extrapresupuestarios ": "Extrapresupuestarios",
                        "Presupuestarios ": "Presupuestarios",
                    }
                )
            )

        if st.button("Guardar cambios en gastos"):
            pk_gastos = "ID_Gasto"  # Ajustar si tu PK es distinta
            updates = guardar_cambios_df(
                tabla="bd_gastos",
                pk_col=pk_gastos,
                df_original=df_g,
                df_editado=edited_df_g,
                columnas_editables=columnas_editables_g_ok,
            )
            st.success(f"Cambios guardados en gastos. Registros actualizados: {updates}")
            st.experimental_rerun()

    else:
        st.info("No hay gastos cargados todavía para este documento.")

    # -------------------------------------------------
    # AGREGAR NUEVO
    # -------------------------------------------------
    st.markdown("### Borrar gastos")
    _delete_rows_ui(
        df=df_g,
        pk_col="ID_Gasto",
        table_name="bd_gastos",
        label="gastos",
        key_prefix="gastos",
        display_cols=["Gasto_Objeto", "Gasto_Categoria"],
    )

    st.markdown("### Agregar nuevo gasto")
    opciones_gasto_objeto = [
        "Gasto en Personal",
        "Bienes de Consumo",
        "Servicios No Personales",
        "Bienes de Uso",
        "Transferencias",
        "Activos Financieros",
        "Servicios de la deuda y disminución de otros pasivos",
        "Otros Gastos",
        "Extrapresupuestario",
    ]

    opciones_gasto_categoria = ["Presupuestarios", "Extrapresupuestarios"]

    with st.form("form_gasto"):
        gasto_categoria = st.selectbox("Categoría del gasto", opciones_gasto_categoria)  # <-- NUEVA
        gasto_objeto = st.selectbox("Objeto del gasto", opciones_gasto_objeto)

        gasto_vigente = st.number_input("Vigente", min_value=0.0, step=1.0)
        gasto_preventivo = st.number_input("Preventivo", min_value=0.0, step=1.0)
        gasto_compromiso = st.number_input("Compromiso", min_value=0.0, step=1.0)
        gasto_devengado = st.number_input("Devengado", min_value=0.0, step=1.0)
        gasto_pagado = st.number_input("Pagado", min_value=0.0, step=1.0)
        gasto_obs = st.text_area("Observaciones")

        submitted_gasto = st.form_submit_button("Guardar gasto")

        if submitted_gasto:
            if not gasto_objeto:
                st.error("Debés seleccionar un objeto de gasto.")
            else:
                nuevo_gasto = {
                    "ID_DocumentoCargado": doc_id_sel,
                    "Gasto_Categoria": gasto_categoria,  # <-- NUEVA
                    "Gasto_Objeto": gasto_objeto,
                    "Gasto_Vigente": gasto_vigente,
                    "Gasto_Preventivo": gasto_preventivo,
                    "Gasto_Compromiso": gasto_compromiso,
                    "Gasto_Devengado": gasto_devengado,
                    "Gasto_Pagado": gasto_pagado,
                    "Gasto_Observacion": _sanitize(gasto_obs),
                }
                supabase.table("bd_gastos").insert(nuevo_gasto).execute()
                st.success("Gasto guardado correctamente.")
                st.experimental_rerun()


# ---------------- TAB JURISDICCIONES ----------------
with tab_jurisdicciones:
    st.subheader("Jurisdicciones del documento")

    res_j = (
        supabase.table("bd_jurisdiccion")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .execute()
    )
    jurisdicciones = res_j.data if res_j.data else []
    df_j = pd.DataFrame(jurisdicciones) if jurisdicciones else pd.DataFrame()

    if jurisdicciones:
        st.markdown("### Editar jurisdicciones existentes")

        df_j = pd.DataFrame(jurisdicciones)

        columnas_editables_j = [
            "Juri_Codigo",
            "Juri_Nombre",
            "Juri_Descripcion",
            "Juri_Observacion",
        ]

        edited_df_j, columnas_editables_j_ok = _make_editor(
            df_j, columnas_editables_j, key="editor_jurisdicciones"
        )

        if st.button("Guardar cambios en jurisdicciones"):
            pk_juri = "ID_Jurisdiccion"  # Ajustar si tu PK es distinta
            updates = guardar_cambios_df(
                tabla="bd_jurisdiccion",
                pk_col=pk_juri,
                df_original=df_j,
                df_editado=edited_df_j,
                columnas_editables=columnas_editables_j_ok,
            )
            st.success(f"Cambios guardados en jurisdicciones. Registros actualizados: {updates}")
            st.experimental_rerun()

    else:
        st.info("No hay jurisdicciones cargadas para este documento.")

    st.markdown("### Borrar jurisdicciones")
    _delete_rows_ui(
        df=df_j,
        pk_col="ID_Jurisdiccion",
        table_name="bd_jurisdiccion",
        label="jurisdicciones",
        key_prefix="jurisdicciones",
        display_cols=["Juri_Codigo", "Juri_Nombre"],
    )

    st.markdown("### Agregar nueva jurisdicción")

    with st.form("form_jurisdiccion"):
        juri_codigo = st.text_input("Código de la jurisdicción", "")
        juri_nombre = st.text_input("Nombre de la jurisdicción", "")
        juri_descripcion = st.text_area("Descripción", "")
        juri_obs = st.text_area("Observaciones", "")

        submitted_juri = st.form_submit_button("Guardar jurisdicción")

        if submitted_juri:
            if not juri_nombre:
                st.error("El nombre de la jurisdicción es obligatorio.")
            else:
                nueva_juri = {
                    "ID_DocumentoCargado": doc_id_sel,
                    "Juri_Codigo": _sanitize(juri_codigo),
                    "Juri_Nombre": _sanitize(juri_nombre),
                    "Juri_Descripcion": _sanitize(juri_descripcion),
                    "Juri_Observacion": _sanitize(juri_obs),
                }
                supabase.table("bd_jurisdiccion").insert(nueva_juri).execute()
                st.success("Jurisdicción guardada correctamente.")
                st.experimental_rerun()

# ---------------- TAB PROGRAMAS ----------------
with tab_programas:
    st.subheader("Programas del documento")

    res_j = (
        supabase.table("bd_jurisdiccion")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .execute()
    )
    jurisdicciones = res_j.data if res_j.data else []
    df_j = pd.DataFrame(jurisdicciones) if jurisdicciones else pd.DataFrame()

    if not jurisdicciones:
        st.info("Primero cargá al menos una jurisdicción para poder asignar programas.")
    else:
        juri_ids = [j["ID_Jurisdiccion"] for j in jurisdicciones]

        res_p = (
            supabase.table("bd_programas")
            .select("*")
            .in_("ID_Jurisdiccion", juri_ids)
            .execute()
        )
        programas = res_p.data if res_p.data else []
        df_p = pd.DataFrame(programas) if programas else pd.DataFrame()
        if not df_p.empty:
            juri_por_id = {
                j["ID_Jurisdiccion"]: j.get("Juri_Codigo") for j in jurisdicciones
            }
            df_p["Juri_Codigo"] = df_p["ID_Jurisdiccion"].map(juri_por_id)
            if "ID_Jurisdiccion" in df_p.columns:
                insert_pos = df_p.columns.get_loc("ID_Jurisdiccion") + 1
                juri_col = df_p.pop("Juri_Codigo")
                df_p.insert(insert_pos, "Juri_Codigo", juri_col)

        if programas:
            tot_vigente = sum(float(p.get("Prog_Vigente") or 0) for p in programas)
            tot_preventivo = sum(float(p.get("Prog_Preventivo") or 0) for p in programas)
            tot_compromiso = sum(float(p.get("Prog_Compromiso") or 0) for p in programas)
            tot_devengado = sum(float(p.get("Prog_Devengado") or 0) for p in programas)
            tot_pagado = sum(float(p.get("Prog_Pagado") or 0) for p in programas)

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Vigente", f"{tot_vigente:,.2f}")
            c2.metric("Preventivo", f"{tot_preventivo:,.2f}")
            c3.metric("Compromiso", f"{tot_compromiso:,.2f}")
            c4.metric("Devengado", f"{tot_devengado:,.2f}")
            c5.metric("Pagado", f"{tot_pagado:,.2f}")

            st.markdown("### Editar programas existentes")

            columnas_editables_p = [
                "Prog_Codigo",
                "Prog_Nombre",
                "Prog_Vigente",
                "Prog_Preventivo",
                "Prog_Compromiso",
                "Prog_Devengado",
                "Prog_Pagado",
                "Prog_Observacion",
            ]

            edited_df_p, columnas_editables_p_ok = _make_editor(
                df_p, columnas_editables_p, key="editor_programas"
            )

            if st.button("Guardar cambios en programas"):
                pk_prog = "ID_Programa"  # Ajustar si tu PK es distinta
                updates = guardar_cambios_df(
                    tabla="bd_programas",
                    pk_col=pk_prog,
                    df_original=df_p,
                    df_editado=edited_df_p,
                    columnas_editables=columnas_editables_p_ok,
                )
                st.success(f"Cambios guardados en programas. Registros actualizados: {updates}")
                st.experimental_rerun()

        else:
            st.info("No hay programas cargados todavía para este documento.")

        st.markdown("### Borrar programas")
        _delete_rows_ui(
            df=df_p,
            pk_col="ID_Programa",
            table_name="bd_programas",
            label="programas",
            key_prefix="programas",
            display_cols=["Prog_Codigo", "Prog_Nombre"],
        )

        st.markdown("### Agregar nuevo programa")

        opciones_juri = {
            f'{j.get("Juri_Codigo","")} - {j.get("Juri_Nombre","")}': j["ID_Jurisdiccion"]
            for j in jurisdicciones
        }

        with st.form("form_programa"):
            juri_sel_nombre = st.selectbox("Jurisdicci3n", list(opciones_juri.keys()))
            prog_codigo = st.text_input("C3digo de programa", "")
            prog_nombre = st.text_input("Nombre del programa", "")
            prog_vigente = st.number_input("Vigente", min_value=0.0, step=1.0)
            prog_preventivo = st.number_input("Preventivo", min_value=0.0, step=1.0)
            prog_compromiso = st.number_input("Compromiso", min_value=0.0, step=1.0)
            prog_devengado = st.number_input("Devengado", min_value=0.0, step=1.0)
            prog_pagado = st.number_input("Pagado", min_value=0.0, step=1.0)
            prog_obs = st.text_area("Observaciones", "")

            submitted_prog = st.form_submit_button("Guardar programa")

            if submitted_prog:
                if not prog_nombre:
                    st.error("El nombre del programa es obligatorio.")
                else:
                    id_juri_sel = opciones_juri[juri_sel_nombre]
                    nuevo_prog = {
                        "ID_Jurisdiccion": id_juri_sel,
                        "Prog_Codigo": _sanitize(prog_codigo),
                        "Prog_Nombre": _sanitize(prog_nombre),
                        "Prog_Vigente": prog_vigente,
                        "Prog_Preventivo": prog_preventivo,
                        "Prog_Compromiso": prog_compromiso,
                        "Prog_Devengado": prog_devengado,
                        "Prog_Pagado": prog_pagado,
                        "Prog_Observacion": _sanitize(prog_obs),
                    }
                    supabase.table("bd_programas").insert(nuevo_prog).execute()
                    st.success("Programa guardado correctamente.")
                    st.experimental_rerun()

# ---------------- TAB SITUACIÓN PATRIMONIAL ----------------
with tab_sitpat:
    st.subheader("Situación patrimonial")

    if not doc_id_sel:
        st.error("No hay documento seleccionado.")
        st.stop()

    if not id_muni_sel:
        st.error("No hay municipio seleccionado.")
        st.stop()

    res_sp = (
        supabase.table("bd_situacionpatrimonial")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .eq("ID_Municipio", id_muni_sel)
        .execute()
    )
    sitpats = res_sp.data if res_sp.data else []
    df_sp = pd.DataFrame(sitpats) if sitpats else pd.DataFrame()

    if sitpats:
        df_sp = pd.DataFrame(sitpats)

        # -------------------------
        # Asegurar columnas
        # -------------------------
        if "SitPat_Tipo" not in df_sp.columns:
            df_sp["SitPat_Tipo"] = None
        if "SitPat_Saldo" not in df_sp.columns:
            df_sp["SitPat_Saldo"] = 0.0

        df_sp["SitPat_Saldo"] = pd.to_numeric(df_sp["SitPat_Saldo"], errors="coerce").fillna(0)

        # -------------------------
        # Totales por tipo
        # -------------------------
        tipo_norm = (
            df_sp["SitPat_Tipo"]
            .astype(str)
            .str.strip()
            .replace({"None": "", "nan": "", "NaN": ""})
        )

        # Normalizamos variantes (por si hay datos viejos con minúsculas)
        tipo_norm = tipo_norm.str.lower().map({
            "activo": "Activo",
            "pasivo": "Pasivo",
            "patrimonio público": "Patrimonio Público",
            "patrimonio publico": "Patrimonio Público",
            "patrimonio": "Patrimonio Público",
        }).fillna("")

        df_sp["_TipoNorm"] = tipo_norm

        total_activo = df_sp.loc[df_sp["_TipoNorm"] == "Activo", "SitPat_Saldo"].sum()
        total_pasivo = df_sp.loc[df_sp["_TipoNorm"] == "Pasivo", "SitPat_Saldo"].sum()
        total_patrimonio = df_sp.loc[df_sp["_TipoNorm"] == "Patrimonio Público", "SitPat_Saldo"].sum()
        total_pasivo_patrimonio = total_pasivo + total_patrimonio

        st.markdown("### Totales del municipio y documento seleccionado")
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Total Activo", f"{total_activo:,.2f}")
        with c2:
            st.metric("Total Pasivo + Patrimonio Público", f"{total_pasivo_patrimonio:,.2f}")

        # -------------------------
        # Editor
        # -------------------------
        st.markdown("### Editar situación patrimonial")

        columnas_editables_sp = [
            "SitPat_Tipo",          # NUEVA / clave para el balance
            "SitPat_Codigo",
            "SitPat_Nombre",
            "SitPat_Saldo",
            "SitPat_Observacion",
        ]
        columnas_editables_sp = [c for c in columnas_editables_sp if c in df_sp.columns]

        edited_df_sp, columnas_editables_sp_ok = _make_editor(
            df_sp.drop(columns=["_TipoNorm"], errors="ignore"),
            columnas_editables_sp,
            key="editor_sitpat"
        )

        if st.button("Guardar cambios en situación patrimonial"):
            pk_sp = "ID_SituacionPatrimonial"  # Ajustar si tu PK es distinta
            updates = guardar_cambios_df(
                tabla="bd_situacionpatrimonial",
                pk_col=pk_sp,
                df_original=df_sp.drop(columns=["_TipoNorm"], errors="ignore"),
                df_editado=edited_df_sp,
                columnas_editables=columnas_editables_sp_ok,
            )
            st.success(f"Cambios guardados. Registros actualizados: {updates}")
            st.experimental_rerun()

    else:
        st.info("No hay situación patrimonial cargada para este municipio y documento.")

    # -------------------------
    # Alta
    # -------------------------
    st.markdown("### Borrar situacion patrimonial")
    _delete_rows_ui(
        df=df_sp,
        pk_col="ID_SituacionPatrimonial",
        table_name="bd_situacionpatrimonial",
        label="situacion patrimonial",
        key_prefix="sitpat",
        display_cols=["SitPat_Tipo", "SitPat_Nombre"],
    )

    st.markdown("### Agregar registro de situación patrimonial")

    with st.form("form_sitpat"):
        sp_tipo = st.selectbox("Tipo", ["Activo", "Pasivo", "Patrimonio Público"])
        sp_codigo = st.text_input("Código", "")
        sp_nombre = st.text_input("Nombre", "")
        sp_saldo = st.number_input("Saldo", step=1.0)
        sp_obs = st.text_area("Observaciones", "")

        submitted_sp = st.form_submit_button("Guardar registro patrimonial")

        if submitted_sp:
            if not sp_nombre:
                st.error("El nombre es obligatorio.")
            else:
                nuevo_sp = {
                    "ID_Municipio": id_muni_sel,
                    "ID_DocumentoCargado": doc_id_sel,
                    "SitPat_Tipo": sp_tipo,  # <-- CLAVE
                    "SitPat_Codigo": _sanitize(sp_codigo),
                    "SitPat_Nombre": _sanitize(sp_nombre),
                    "SitPat_Saldo": sp_saldo,
                    "SitPat_Observacion": _sanitize(sp_obs),
                }

                try:
                    supabase.table("bd_situacionpatrimonial").insert(nuevo_sp).execute()
                    st.success("Registro patrimonial guardado correctamente.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error("Error al guardar el registro en Supabase.")
                    st.exception(e)

# ---------------- TAB TESORERÍA ----------------
with tab_tesoreria:
    st.subheader("Movimientos de tesorería")

    res_mt = (
        supabase.table("bd_movimientosTesoreria")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .eq("ID_Municipio", id_muni_sel)
        .execute()
    )
    movs = res_mt.data if res_mt.data else []
    df_mt = pd.DataFrame(movs) if movs else pd.DataFrame()

    if movs:
        df_mt = pd.DataFrame(movs)

        # -------------------------
        # Totales (Saldo Inicial / Ingreso / Egreso / Saldo Final)
        # -------------------------
        if "MovTes_TipoResumido" not in df_mt.columns:
            df_mt["MovTes_TipoResumido"] = None
        if "MovTes_Tipo" not in df_mt.columns:
            df_mt["MovTes_Tipo"] = None
        if "MovTes_Importe" not in df_mt.columns:
            df_mt["MovTes_Importe"] = 0.0

        df_mt["MovTes_Importe"] = pd.to_numeric(df_mt["MovTes_Importe"], errors="coerce").fillna(0)

        # Normalización + fallback
        tipo_res = df_mt["MovTes_TipoResumido"].astype(str).str.strip()
        tipo_res = tipo_res.replace({"None": "", "nan": "", "NaN": ""})

        tipo_det = df_mt["MovTes_Tipo"].astype(str).str.strip().str.lower()

        tipo_inferido = pd.Series([""] * len(df_mt))
        # inferencias por palabras clave (por si hay registros viejos)
        tipo_inferido[tipo_det.str.contains("saldo", na=False)] = "Saldo Inicial"
        tipo_inferido[tipo_det.str.contains("inicial", na=False)] = "Saldo Inicial"
        tipo_inferido[tipo_det.str.contains("ingreso", na=False)] = "Ingreso"
        tipo_inferido[tipo_det.str.contains("egreso", na=False)] = "Egreso"

        tipo_final = tipo_res.copy()
        mask_vacio = (tipo_final == "")
        tipo_final[mask_vacio] = tipo_inferido[mask_vacio]

        # normalizamos variantes comunes
        tipo_final = (
            tipo_final.astype(str)
            .str.strip()
            .str.lower()
            .map({
                "saldo inicial": "Saldo Inicial",
                "saldo": "Saldo Inicial",
                "inicial": "Saldo Inicial",
                "ingreso": "Ingreso",
                "egreso": "Egreso",
            })
            .fillna("")
        )

        df_mt["_TipoResFinal"] = tipo_final

        saldo_inicial = df_mt.loc[df_mt["_TipoResFinal"] == "Saldo Inicial", "MovTes_Importe"].sum()
        total_ingreso = df_mt.loc[df_mt["_TipoResFinal"] == "Ingreso", "MovTes_Importe"].sum()
        total_egreso  = df_mt.loc[df_mt["_TipoResFinal"] == "Egreso",  "MovTes_Importe"].sum()
        saldo_final = saldo_inicial + total_ingreso - total_egreso

        st.markdown("### Totales del documento (Tesorería)")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Saldo Inicial", f"{saldo_inicial:,.2f}")
        with c2:
            st.metric("Total Ingresos", f"{total_ingreso:,.2f}")
        with c3:
            st.metric("Total Egresos", f"{total_egreso:,.2f}")
        with c4:
            st.metric("Saldo Final", f"{saldo_final:,.2f}")

        # -------------------------
        # Editor
        # -------------------------
        st.markdown("### Editar movimientos cargados")

        columnas_editables_mt = [
            "MovTes_TipoResumido",   # Saldo Inicial / Ingreso / Egreso
            "MovTes_Tipo",           # Detalle libre
            "MovTes_Importe",
            "MovTes_Observacion",
        ]
        columnas_editables_mt = [c for c in columnas_editables_mt if c in df_mt.columns]

        edited_df_mt, columnas_editables_mt_ok = _make_editor(
            df_mt.drop(columns=["_TipoResFinal"], errors="ignore"),
            columnas_editables_mt,
            key="editor_tesoreria"
        )

        if st.button("Guardar cambios en tesorería"):
            pk_mt = "ID_MovimientoTesoreria"  # Ajustar si tu PK es distinta
            updates = guardar_cambios_df(
                tabla="bd_movimientosTesoreria",
                pk_col=pk_mt,
                df_original=df_mt.drop(columns=["_TipoResFinal"], errors="ignore"),
                df_editado=edited_df_mt,
                columnas_editables=columnas_editables_mt_ok,
            )
            st.success(f"Cambios guardados. Registros actualizados: {updates}")
            st.experimental_rerun()

    else:
        st.info("No hay movimientos de tesorería cargados para este documento.")

    # -------------------------
    # Alta
    # -------------------------
    st.markdown("### Borrar movimientos de tesoreria")
    _delete_rows_ui(
        df=df_mt,
        pk_col="ID_MovimientosTesoreria",
        table_name="bd_movimientosTesoreria",
        label="movimientos de tesoreria",
        key_prefix="tesoreria",
        display_cols=["MovTes_TipoResumido", "MovTes_Tipo"],
    )


    st.markdown("### Agregar movimiento de tesorería")

    with st.form("form_movtes"):
        mov_tipo_resumido = st.selectbox("Tipo resumido", ["Saldo Inicial", "Ingreso", "Egreso"])
        mov_tipo = st.text_input("Detalle / tipo (opcional)", "")
        mov_importe = st.number_input("Importe", min_value=0.0, step=1.0)
        mov_obs = st.text_area("Observaciones", "")

        submitted_mt = st.form_submit_button("Guardar movimiento")

        if submitted_mt:
            if not doc_id_sel:
                st.error("No hay documento seleccionado (ID_DocumentoCargado vacío).")
            elif not id_muni_sel:
                st.error("No hay municipio seleccionado (ID_Municipio vacío).")
            else:
                nuevo_mt = {
                    "ID_Municipio": id_muni_sel,
                    "ID_DocumentoCargado": doc_id_sel,
                    "MovTes_TipoResumido": mov_tipo_resumido,
                    "MovTes_Tipo": _sanitize(mov_tipo),
                    "MovTes_Importe": mov_importe,
                    "MovTes_Observacion": _sanitize(mov_obs),
                }

                try:
                    supabase.table("bd_movimientosTesoreria").insert(nuevo_mt).execute()
                    st.success("Movimiento guardado correctamente.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error("Error al guardar el movimiento en Supabase.")
                    st.exception(e)

# ---------------- TAB CUENTAS ----------------
with tab_cuentas:
    st.subheader("Cuentas")

    res_c = (
        supabase.table("bd_cuentas")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .eq("ID_Municipio", id_muni_sel)  # <-- para que sea "por municipio"
        .execute()
    )
    cuentas = res_c.data if res_c.data else []
    df_c = pd.DataFrame(cuentas) if cuentas else pd.DataFrame()

    if cuentas:
        df_c = pd.DataFrame(cuentas)

        # -------------------------
        # Tarjeta total por municipio (del documento seleccionado)
        # -------------------------
        if "Cuenta_Importe" not in df_c.columns:
            df_c["Cuenta_Importe"] = 0.0

        df_c["Cuenta_Importe"] = pd.to_numeric(df_c["Cuenta_Importe"], errors="coerce").fillna(0)
        total_cuentas_muni = df_c["Cuenta_Importe"].sum()

        st.markdown("### Totales")
        st.metric("Total cuentas (municipio)", f"{total_cuentas_muni:,.2f}")

        # -------------------------
        # Editor
        # -------------------------
        st.markdown("### Editar cuentas existentes")

        columnas_editables_c = [
            "Cuenta_Codigo",
            "Cuenta_Nombre",
            "Cuenta_Importe",
        ]
        columnas_editables_c = [c for c in columnas_editables_c if c in df_c.columns]

        edited_df_c, columnas_editables_c_ok = _make_editor(
            df_c, columnas_editables_c, key="editor_cuentas"
        )

        if st.button("Guardar cambios en cuentas"):
            pk_c = "ID_Cuenta"  # Ajustar si tu PK es distinta
            updates = guardar_cambios_df(
                tabla="bd_cuentas",
                pk_col=pk_c,
                df_original=df_c,
                df_editado=edited_df_c,
                columnas_editables=columnas_editables_c_ok,
            )
            st.success(f"Cambios guardados. Registros actualizados: {updates}")
            st.experimental_rerun()

    else:
        st.info("No hay cuentas cargadas para este municipio y documento.")

    st.markdown("### Borrar cuentas")
    _delete_rows_ui(
        df=df_c,
        pk_col="ID_Cuenta",
        table_name="bd_cuentas",
        label="cuentas",
        key_prefix="cuentas",
        display_cols=["Cuenta_Codigo", "Cuenta_Nombre"],
    )

    st.markdown("### Agregar cuenta")

    with st.form("form_cuenta"):
        cuenta_codigo = st.text_input("Codigo de cuenta", "")
        cuenta_nombre = st.text_input("Nombre de la cuenta", "")
        cuenta_importe = st.number_input("Importe", step=1.0)
        submitted_c = st.form_submit_button("Guardar cuenta")

        if submitted_c:
            if not cuenta_nombre:
                st.error("El nombre de la cuenta es obligatorio.")
            elif not doc_id_sel:
                st.error("No hay documento seleccionado.")
            elif not id_muni_sel:
                st.error("No hay municipio seleccionado.")
            else:
                nueva_cuenta = {
                    "ID_Municipio": id_muni_sel,
                    "ID_DocumentoCargado": doc_id_sel,
                    "Cuenta_Codigo": _sanitize(cuenta_codigo),
                    "Cuenta_Nombre": _sanitize(cuenta_nombre),
                    "Cuenta_Importe": cuenta_importe,
                }

                try:
                    supabase.table("bd_cuentas").insert(nueva_cuenta).execute()
                    st.success("Cuenta guardada correctamente.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error("Error al guardar la cuenta en Supabase.")
                    st.exception(e)
with tab_metas:
    st.subheader("Metas por programa")

    res_j = (
        supabase.table("bd_jurisdiccion")
        .select("*")
        .eq("ID_DocumentoCargado", doc_id_sel)
        .execute()
    )
    jurisdicciones = res_j.data if res_j.data else []
    df_j = pd.DataFrame(jurisdicciones) if jurisdicciones else pd.DataFrame()

    if not jurisdicciones:
        st.info("Primero cargá jurisdicciones y programas para poder asignar metas.")
    else:
        juri_ids = [j["ID_Jurisdiccion"] for j in jurisdicciones]

        res_p = (
            supabase.table("bd_programas")
            .select("*")
            .in_("ID_Jurisdiccion", juri_ids)
            .execute()
        )
        programas = res_p.data if res_p.data else []
        df_p = pd.DataFrame(programas) if programas else pd.DataFrame()

        if not programas:
            st.info("Todavía no hay programas cargados para asignar metas.")
        else:
            prog_ids = [p["ID_Programa"] for p in programas]

            res_metas = (
                supabase.table("bd_metas")
                .select("*")
                .in_("ID_Programa", prog_ids)
                .execute()
            )
            metas = res_metas.data if res_metas.data else []
            df_m = pd.DataFrame(metas) if metas else pd.DataFrame()

            if metas:
                st.markdown("### Editar metas existentes")

                df_m = pd.DataFrame(metas)

                # Columnas editables reales en bd_metas
                columnas_editables_m = [
                    "Meta_Nombre",
                    "Meta_Unidad",
                    "Meta_Anual",
                    "Meta_Parcial",
                    "Meta_Ejecutado",
                    "Meta_Observacion",
                ]

                # Por si alguna columna viene faltante en el DF por datos viejos/nulls
                columnas_editables_m = [c for c in columnas_editables_m if c in df_m.columns]

                edited_df_m, columnas_editables_m_ok = _make_editor(
                    df_m, columnas_editables_m, key="editor_metas"
                )

                if st.button("Guardar cambios en metas"):
                    pk_m = "ID_Meta"
                    updates = guardar_cambios_df(
                        tabla="bd_metas",
                        pk_col=pk_m,
                        df_original=df_m,
                        df_editado=edited_df_m,
                        columnas_editables=columnas_editables_m_ok,
                    )
                    st.success(f"Cambios guardados. Registros actualizados: {updates}")
                    st.experimental_rerun()
            else:
                st.info("No hay metas cargadas todavía para este documento.")

            st.markdown("### Borrar metas")
            _delete_rows_ui(
                df=df_m,
                pk_col="ID_Meta",
                table_name="bd_metas",
                label="metas",
                key_prefix="metas",
                display_cols=["Meta_Nombre", "Meta_Unidad"],
            )

            st.markdown("### Agregar nueva meta")

            opciones_prog = {
                f'{p.get("Prog_Codigo","")} - {p.get("Prog_Nombre","")}': p["ID_Programa"]
                for p in programas
            }

            with st.form("form_meta"):
                prog_sel_nombre = st.selectbox("Programa", list(opciones_prog.keys()))

                meta_nombre = st.text_input("Nombre de la meta", "")
                meta_unidad = st.text_input("Unidad", "")
                meta_anual = st.number_input("Meta anual", step=1.0)
                meta_parcial = st.number_input("Meta parcial", step=1.0)
                meta_ejecutado = st.number_input("Meta ejecutado", step=1.0)
                meta_obs = st.text_area("Observaciones", "")

                submitted_meta = st.form_submit_button("Guardar meta")

                if submitted_meta:
                    if not meta_nombre:
                        st.error("El nombre de la meta es obligatorio.")
                    else:
                        id_prog_sel = opciones_prog[prog_sel_nombre]

                        nueva_meta = {
                            "ID_Programa": id_prog_sel,
                            "Meta_Nombre": _sanitize(meta_nombre),
                            "Meta_Unidad": _sanitize(meta_unidad),
                            "Meta_Anual": meta_anual,
                            "Meta_Parcial": meta_parcial,
                            "Meta_Ejecutado": meta_ejecutado,
                            "Meta_Observacion": _sanitize(meta_obs),
                        }

                        supabase.table("bd_metas").insert(nueva_meta).execute()
                        st.success("Meta guardada correctamente.")
                        st.experimental_rerun()
