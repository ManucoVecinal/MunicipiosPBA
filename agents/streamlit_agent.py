# -*- coding: utf-8 -*-
"""
StreamlitAgent: Agente para desarrollo y testing de UI Streamlit.

Funciones principales:
- Validación de session state
- Generación de boilerplate para tabs/forms
- Generación de datos mock para testing
- Validación de configuraciones de componentes
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type
import random
import uuid

from .base import BaseAgent
from .config import AgentConfig


@dataclass
class SessionStateIssue:
    """Issue encontrado en session state."""
    key: str
    issue_type: str  # "missing", "stale", "type_mismatch", "inconsistent"
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    recommendation: str = ""


@dataclass
class ComponentConfig:
    """Configuración de un componente UI."""
    component_type: str
    name: str
    table_name: Optional[str] = None
    columns: List[str] = None
    editable_columns: List[str] = None
    pk_column: Optional[str] = None


class StreamlitAgent(BaseAgent):
    """Agente para desarrollo de UI Streamlit."""

    # Configuración esperada de session state
    EXPECTED_SESSION_KEYS = {
        "user": {
            "type": "dict_or_object",
            "required": True,
            "description": "Usuario autenticado",
        },
        "municipio_seleccionado_id": {
            "type": "str",
            "required": False,
            "description": "ID del municipio seleccionado",
        },
        "municipio_seleccionado_nombre": {
            "type": "str",
            "required": False,
            "description": "Nombre del municipio seleccionado",
        },
        "documento_seleccionado_id": {
            "type": "str",
            "required": False,
            "description": "ID del documento seleccionado",
        },
        "documento_seleccionado_nombre": {
            "type": "str",
            "required": False,
            "description": "Nombre del documento seleccionado",
        },
    }

    # Configuraciones de tablas para UI
    TABLE_UI_CONFIGS = {
        "bd_recursos": {
            "pk": "ID_Recurso",
            "display": ["Rec_Nombre", "Rec_Tipo", "Rec_Categoria"],
            "editable": [
                "Rec_Nombre", "Rec_Tipo", "Rec_Categoria",
                "Rec_Vigente", "Rec_Devengado", "Rec_Percibido",
                "Rec_Observacion",
            ],
            "metrics": ["Rec_Vigente", "Rec_Devengado", "Rec_Percibido"],
        },
        "bd_gastos": {
            "pk": "ID_Gasto",
            "display": ["Gasto_Objeto", "Gasto_Categoria"],
            "editable": [
                "Gasto_Objeto", "Gasto_Categoria",
                "Gasto_Vigente", "Gasto_Preventivo", "Gasto_Compromiso",
                "Gasto_Devengado", "Gasto_Pagado",
            ],
            "metrics": ["Gasto_Vigente", "Gasto_Devengado", "Gasto_Pagado"],
        },
        "bd_jurisdiccion": {
            "pk": "ID_Jurisdiccion",
            "display": ["Juri_Codigo", "Juri_Nombre"],
            "editable": ["Juri_Codigo", "Juri_Nombre", "Juri_Descripcion"],
            "metrics": [],
        },
        "bd_programas": {
            "pk": "ID_Programa",
            "display": ["Prog_Codigo", "Prog_Nombre"],
            "editable": [
                "Prog_Codigo", "Prog_Nombre",
                "Prog_Vigente", "Prog_Preventivo", "Prog_Compromiso",
                "Prog_Devengado", "Prog_Pagado",
            ],
            "metrics": ["Prog_Vigente", "Prog_Devengado", "Prog_Pagado"],
        },
        "bd_situacionpatrimonial": {
            "pk": "ID_SituacionPatrimonial",
            "display": ["SitPat_Tipo", "SitPat_Nombre"],
            "editable": [
                "SitPat_Tipo", "SitPat_Codigo", "SitPat_Nombre", "SitPat_Saldo",
            ],
            "metrics": ["SitPat_Saldo"],
        },
        "bd_movimientosTesoreria": {
            "pk": "ID_MovimientoTesoreria",
            "display": ["MovTes_TipoResumido", "MovTes_Tipo"],
            "editable": ["MovTes_TipoResumido", "MovTes_Tipo", "MovTes_Importe"],
            "metrics": ["MovTes_Importe"],
        },
        "bd_cuentas": {
            "pk": "ID_Cuenta",
            "display": ["Cuenta_Codigo", "Cuenta_Nombre"],
            "editable": ["Cuenta_Codigo", "Cuenta_Nombre", "Cuenta_Importe"],
            "metrics": ["Cuenta_Importe"],
        },
        "bd_metas": {
            "pk": "ID_Meta",
            "display": ["Meta_Nombre", "Meta_Unidad"],
            "editable": [
                "Meta_Nombre", "Meta_Unidad",
                "Meta_Anual", "Meta_Parcial", "Meta_Ejecutado",
            ],
            "metrics": ["Meta_Anual", "Meta_Ejecutado"],
        },
    }

    def __init__(self, config: AgentConfig = None, verbose: bool = False):
        """
        Inicializa el StreamlitAgent.

        Args:
            config: Configuración del agente
            verbose: Modo verbose
        """
        super().__init__(name="streamlit", verbose=verbose)
        self.config = config or AgentConfig()

    def health_check(self) -> Dict[str, Any]:
        """
        Verifica disponibilidad del agente.

        Returns:
            Dict con status
        """
        return {
            "status": "ok",
            "message": "StreamlitAgent disponible",
            "details": {
                "tables_configured": len(self.TABLE_UI_CONFIGS),
                "session_keys_defined": len(self.EXPECTED_SESSION_KEYS),
            },
        }

    # === Session State Management ===

    def validate_session_state(
        self,
        session_state: Dict[str, Any],
    ) -> List[SessionStateIssue]:
        """
        Valida session state contra estructura esperada.

        Args:
            session_state: Dict con session state actual

        Returns:
            Lista de issues encontrados
        """
        issues = []

        for key, spec in self.EXPECTED_SESSION_KEYS.items():
            if key not in session_state:
                if spec["required"]:
                    issues.append(SessionStateIssue(
                        key=key,
                        issue_type="missing",
                        expected_type=spec["type"],
                        recommendation=f"Inicializar '{key}' en session_state",
                    ))
            else:
                value = session_state[key]
                # Verificar tipo básico
                if spec["type"] == "str" and not isinstance(value, (str, type(None))):
                    issues.append(SessionStateIssue(
                        key=key,
                        issue_type="type_mismatch",
                        expected_type="str",
                        actual_type=type(value).__name__,
                        recommendation=f"'{key}' deberia ser str",
                    ))

        # Verificar consistencia
        if session_state.get("documento_seleccionado_id"):
            if not session_state.get("municipio_seleccionado_id"):
                issues.append(SessionStateIssue(
                    key="documento_seleccionado_id",
                    issue_type="inconsistent",
                    recommendation="Documento seleccionado sin municipio",
                ))

        return issues

    def get_expected_session_keys(self) -> Dict[str, Dict]:
        """Retorna estructura esperada de session state."""
        return self.EXPECTED_SESSION_KEYS.copy()

    def generate_mock_session_state(self, logged_in: bool = True) -> Dict[str, Any]:
        """
        Genera session state mock para testing.

        Args:
            logged_in: Si incluir usuario logueado

        Returns:
            Dict simulando session state
        """
        state = {}

        if logged_in:
            state["user"] = {
                "id": str(uuid.uuid4()),
                "email": "test@ejemplo.com",
                "aud": "authenticated",
            }
        else:
            state["user"] = None

        state["municipio_seleccionado_id"] = str(uuid.uuid4())
        state["municipio_seleccionado_nombre"] = "Municipio de Prueba"
        state["documento_seleccionado_id"] = str(uuid.uuid4())
        state["documento_seleccionado_nombre"] = "SITECO Q1 2025"

        return state

    # === Code Generation ===

    def generate_tab_boilerplate(
        self,
        tab_name: str,
        table_name: str,
    ) -> str:
        """
        Genera código boilerplate para un nuevo tab.

        Args:
            tab_name: Nombre del tab
            table_name: Tabla asociada

        Returns:
            Código Python como string
        """
        config = self.TABLE_UI_CONFIGS.get(table_name, {})
        pk = config.get("pk", "ID")
        editable = config.get("editable", [])
        metrics = config.get("metrics", [])

        code = f'''
# === Tab: {tab_name} ===
with tab_{tab_name.lower()}:
    st.subheader("{tab_name}")

    # Verificar documento seleccionado
    if not doc_id_sel:
        st.warning("Selecciona un documento primero")
    else:
        # Fetch data
        df_{tab_name.lower()} = fetch_data_{tab_name.lower()}(supabase, doc_id_sel)

        if df_{tab_name.lower()}.empty:
            st.info("Sin datos cargados para este documento")
        else:
            # Mostrar metricas
            cols_metrics = st.columns({len(metrics) if metrics else 3})
'''

        # Agregar métricas
        for i, metric in enumerate(metrics[:3]):
            code += f'''            with cols_metrics[{i}]:
                total = df_{tab_name.lower()}["{metric}"].sum()
                st.metric("{metric.replace('_', ' ')}", f"{{total:,.2f}}")
'''

        code += f'''
            # Editor de datos
            st.divider()
            editable_cols = {editable}

            edited_df = st.data_editor(
                df_{tab_name.lower()},
                disabled=[c for c in df_{tab_name.lower()}.columns if c not in editable_cols],
                key="editor_{tab_name.lower()}",
                num_rows="dynamic",
            )

            # Boton guardar
            if st.button("Guardar cambios", key="save_{tab_name.lower()}"):
                updates = guardar_cambios_df(
                    tabla="{table_name}",
                    pk_col="{pk}",
                    df_original=df_{tab_name.lower()},
                    df_editado=edited_df,
                    columnas_editables=editable_cols,
                )
                if updates > 0:
                    st.success(f"{{updates}} registros actualizados")
                    st.rerun()
'''
        return code

    def generate_form_boilerplate(
        self,
        form_name: str,
        fields: List[Dict[str, str]],
    ) -> str:
        """
        Genera código para un formulario.

        Args:
            form_name: Nombre del formulario
            fields: Lista de {name, type, label}

        Returns:
            Código Python
        """
        code = f'''
# === Formulario: {form_name} ===
with st.form("form_{form_name}"):
    st.subheader("Nuevo registro")
'''

        for field in fields:
            name = field.get("name", "campo")
            ftype = field.get("type", "text")
            label = field.get("label", name)

            if ftype == "text":
                code += f'    {name} = st.text_input("{label}")\n'
            elif ftype == "number":
                code += f'    {name} = st.number_input("{label}", min_value=0.0, step=1.0)\n'
            elif ftype == "select":
                options = field.get("options", [])
                code += f'    {name} = st.selectbox("{label}", {options})\n'
            elif ftype == "textarea":
                code += f'    {name} = st.text_area("{label}")\n'

        code += f'''
    submitted = st.form_submit_button("Guardar")

    if submitted:
        # Validar campos requeridos
        if not {fields[0]["name"] if fields else "campo"}:
            st.error("Completa los campos requeridos")
        else:
            data = {{
'''

        for field in fields:
            name = field.get("name", "campo")
            code += f'                "{name}": {name},\n'

        code += f'''            }}
            # Insertar en BD
            try:
                supabase.table("tabla").insert(data).execute()
                st.success("Registro creado exitosamente")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {{e}}")
'''
        return code

    def generate_data_editor_boilerplate(self, table_name: str) -> str:
        """
        Genera código para data editor.

        Args:
            table_name: Nombre de la tabla

        Returns:
            Código Python
        """
        config = self.TABLE_UI_CONFIGS.get(table_name, {})
        pk = config.get("pk", "ID")
        editable = config.get("editable", [])

        return f'''
# === Data Editor: {table_name} ===
df = fetch_rows(supabase, "{table_name}", {{"ID_DocumentoCargado": doc_id_sel}})
df = pd.DataFrame(df)

if df.empty:
    st.info("Sin datos")
else:
    editable_cols = {editable}
    disabled_cols = [c for c in df.columns if c not in editable_cols]

    edited_df = st.data_editor(
        df,
        disabled=disabled_cols,
        key="editor_{table_name}",
        num_rows="fixed",
        use_container_width=True,
    )

    if st.button("Guardar cambios", key="save_{table_name}"):
        updates = guardar_cambios_df(
            tabla="{table_name}",
            pk_col="{pk}",
            df_original=df,
            df_editado=edited_df,
            columnas_editables=editable_cols,
        )
        st.success(f"{{updates}} registros actualizados")
        st.rerun()
'''

    def generate_delete_ui_boilerplate(self, table_name: str) -> str:
        """
        Genera código para UI de eliminación.

        Args:
            table_name: Nombre de la tabla

        Returns:
            Código Python
        """
        config = self.TABLE_UI_CONFIGS.get(table_name, {})
        pk = config.get("pk", "ID")
        display = config.get("display", [])

        return f'''
# === Delete UI: {table_name} ===
st.divider()
with st.expander("Eliminar registros"):
    df_del = df[{display + [pk]}].copy()
    df_del["Eliminar"] = False

    select_all = st.checkbox("Seleccionar todos", key="select_all_{table_name}")
    if select_all:
        df_del["Eliminar"] = True

    edited_del = st.data_editor(
        df_del,
        disabled={display},
        key="delete_editor_{table_name}",
    )

    confirm = st.checkbox("Confirmo eliminar los registros seleccionados")

    if st.button("Eliminar", key="delete_{table_name}"):
        if not confirm:
            st.error("Debes confirmar antes de eliminar")
        else:
            ids_to_delete = edited_del.loc[
                edited_del["Eliminar"] == True, "{pk}"
            ].tolist()

            if ids_to_delete:
                deleted = delete_rows(supabase, "{table_name}", "{pk}", ids_to_delete)
                st.success(f"{{deleted}} registros eliminados")
                st.rerun()
            else:
                st.warning("No hay registros seleccionados")
'''

    def generate_full_crud_tab(self, table_name: str, tab_name: str = None) -> str:
        """
        Genera tab CRUD completo.

        Args:
            table_name: Nombre de la tabla
            tab_name: Nombre del tab (opcional)

        Returns:
            Código Python completo
        """
        if tab_name is None:
            tab_name = table_name.replace("bd_", "").title()

        code = self.generate_tab_boilerplate(tab_name, table_name)
        code += "\n" + self.generate_delete_ui_boilerplate(table_name)
        return code

    # === Mock Data Generation ===

    def generate_mock_municipio(self) -> Dict[str, Any]:
        """Genera datos mock de municipio."""
        nombres = [
            "San Isidro", "Tigre", "Vicente Lopez", "La Plata",
            "Quilmes", "Lomas de Zamora", "Avellaneda", "Moron",
        ]
        return {
            "ID_Municipio": str(uuid.uuid4()),
            "Muni_Nombre": random.choice(nombres),
            "Muni_Poblacion_2022": random.randint(50000, 500000),
            "Muni_Superficie": round(random.uniform(20, 200), 2),
            "Muni_Densidad": round(random.uniform(500, 10000), 2),
        }

    def generate_mock_document(self, muni_id: str = None) -> Dict[str, Any]:
        """Genera datos mock de documento."""
        tipos = ["Rendicion", "Presupuesto"]
        periodos = ["Q1", "Q2", "Q3", "Q4", "Anual"]
        estados = ["Pendiente", "Procesando", "Procesado", "Error"]

        return {
            "ID_DocumentoCargado": str(uuid.uuid4()),
            "ID_Municipio": muni_id or str(uuid.uuid4()),
            "Doc_Tipo": random.choice(tipos),
            "Doc_Periodo": random.choice(periodos),
            "Doc_Anio": random.randint(2020, 2025),
            "Doc_Nombre": f"SITECO {random.choice(periodos)} {random.randint(2020, 2025)}",
            "Doc_Estado": random.choice(estados),
        }

    def generate_mock_table_data(
        self,
        table_name: str,
        count: int = 5,
        doc_id: str = None,
    ) -> List[Dict]:
        """
        Genera datos mock para cualquier tabla.

        Args:
            table_name: Nombre de la tabla
            count: Cantidad de registros
            doc_id: ID de documento (opcional)

        Returns:
            Lista de registros mock
        """
        config = self.TABLE_UI_CONFIGS.get(table_name, {})
        pk = config.get("pk", "ID")

        rows = []
        for i in range(count):
            row = {pk: str(uuid.uuid4())}

            if doc_id:
                row["ID_DocumentoCargado"] = doc_id

            # Generar valores según tabla
            if table_name == "bd_recursos":
                row.update({
                    "Rec_Nombre": f"Recurso {i+1}",
                    "Rec_Tipo": random.choice(["Presupuestarios", "Extrapresupuestarios"]),
                    "Rec_Categoria": random.choice(["Corrientes", "De Capital"]),
                    "Rec_Vigente": round(random.uniform(100000, 5000000), 2),
                    "Rec_Devengado": round(random.uniform(80000, 4000000), 2),
                    "Rec_Percibido": round(random.uniform(60000, 3500000), 2),
                })
            elif table_name == "bd_gastos":
                row.update({
                    "Gasto_Objeto": f"{i+1} - Gasto tipo {i+1}",
                    "Gasto_Categoria": random.choice(["Corriente", "De Capital"]),
                    "Gasto_Vigente": round(random.uniform(100000, 5000000), 2),
                    "Gasto_Preventivo": round(random.uniform(90000, 4500000), 2),
                    "Gasto_Compromiso": round(random.uniform(80000, 4000000), 2),
                    "Gasto_Devengado": round(random.uniform(70000, 3500000), 2),
                    "Gasto_Pagado": round(random.uniform(50000, 3000000), 2),
                })
            elif table_name == "bd_jurisdiccion":
                row.update({
                    "Juri_Codigo": f"{i+1:02d}",
                    "Juri_Nombre": f"Jurisdiccion {i+1}",
                })
            elif table_name == "bd_programas":
                row.update({
                    "Prog_Codigo": f"{i+1:02d}",
                    "Prog_Nombre": f"Programa {i+1}",
                    "Prog_Vigente": round(random.uniform(50000, 2000000), 2),
                    "Prog_Devengado": round(random.uniform(40000, 1800000), 2),
                })
            else:
                # Genérico
                for col in config.get("editable", []):
                    if "importe" in col.lower() or "saldo" in col.lower():
                        row[col] = round(random.uniform(10000, 500000), 2)
                    elif "codigo" in col.lower():
                        row[col] = f"{i+1:02d}"
                    elif "nombre" in col.lower():
                        row[col] = f"Item {i+1}"

            rows.append(row)

        return rows

    # === Validation ===

    def validate_data_editor_config(
        self,
        table_name: str,
        columns: List[str],
        editable_columns: List[str],
    ) -> List[str]:
        """
        Valida configuración de data editor.

        Args:
            table_name: Tabla
            columns: Columnas del DataFrame
            editable_columns: Columnas marcadas editables

        Returns:
            Lista de warnings/errors
        """
        issues = []
        config = self.TABLE_UI_CONFIGS.get(table_name)

        if not config:
            issues.append(f"Tabla '{table_name}' no tiene configuracion UI")
            return issues

        pk = config.get("pk")
        if pk in editable_columns:
            issues.append(f"PK '{pk}' no deberia ser editable")

        expected_editable = set(config.get("editable", []))
        actual_editable = set(editable_columns)

        missing = expected_editable - actual_editable - {pk}
        if missing:
            issues.append(f"Columnas editables faltantes: {missing}")

        extra = actual_editable - expected_editable
        if extra:
            issues.append(f"Columnas extra marcadas editables: {extra}")

        return issues

    def get_table_ui_config(self, table_name: str) -> Optional[Dict]:
        """Obtiene configuración UI de una tabla."""
        return self.TABLE_UI_CONFIGS.get(table_name)

    def list_available_tables(self) -> List[str]:
        """Lista tablas con configuración UI."""
        return list(self.TABLE_UI_CONFIGS.keys())
