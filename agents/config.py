# -*- coding: utf-8 -*-
"""
Gestión de configuración para los agentes.
Soporta carga desde Streamlit secrets, variables de entorno, o valores directos.
"""

from dataclasses import dataclass, field
from typing import Any, Optional
import os


@dataclass
class AgentConfig:
    """Configuración para los agentes."""

    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None
    verbose: bool = False
    log_file: Optional[str] = None
    pdf_directory: str = "PDFS"

    # Definiciones de tablas del proyecto
    tables: dict = field(default_factory=lambda: {
        "bd_municipios": {
            "pk": "ID_Municipio",
            "fk": [],
            "description": "Municipios de Buenos Aires",
        },
        "BD_DocumentosCargados": {
            "pk": "ID_DocumentoCargado",
            "fk": [("ID_Municipio", "bd_municipios")],
            "description": "Documentos PDF cargados",
        },
        "bd_recursos": {
            "pk": "ID_Recurso",
            "fk": [("ID_DocumentoCargado", "BD_DocumentosCargados")],
            "description": "Recursos/ingresos",
        },
        "bd_gastos": {
            "pk": "ID_Gasto",
            "fk": [("ID_DocumentoCargado", "BD_DocumentosCargados")],
            "description": "Gastos/egresos",
        },
        "bd_jurisdiccion": {
            "pk": "ID_Jurisdiccion",
            "fk": [("ID_DocumentoCargado", "BD_DocumentosCargados")],
            "description": "Jurisdicciones administrativas",
        },
        "bd_programas": {
            "pk": "ID_Programa",
            "fk": [("ID_Jurisdiccion", "bd_jurisdiccion")],
            "description": "Programas de gobierno",
        },
        "bd_situacionpatrimonial": {
            "pk": "ID_SituacionPatrimonial",
            "fk": [("ID_DocumentoCargado", "BD_DocumentosCargados")],
            "description": "Situacion patrimonial (activo/pasivo)",
        },
        "bd_movimientosTesoreria": {
            "pk": "ID_MovimientoTesoreria",
            "fk": [("ID_DocumentoCargado", "BD_DocumentosCargados")],
            "description": "Movimientos de tesoreria",
        },
        "bd_cuentas": {
            "pk": "ID_Cuenta",
            "fk": [("ID_DocumentoCargado", "BD_DocumentosCargados")],
            "description": "Cuentas bancarias/caja",
        },
        "bd_metas": {
            "pk": "ID_Meta",
            "fk": [("ID_Programa", "bd_programas")],
            "description": "Metas/objetivos de programas",
        },
    })

    @classmethod
    def from_streamlit_secrets(cls, secrets_path: str = None) -> "AgentConfig":
        """
        Carga configuración desde Streamlit secrets.

        Args:
            secrets_path: Ruta al archivo secrets.toml.
                          Por defecto busca en .streamlit/secrets.toml

        Returns:
            AgentConfig con credenciales de Supabase
        """
        if secrets_path is None:
            # Buscar en ubicaciones comunes
            possible_paths = [
                ".streamlit/secrets.toml",
                os.path.join(os.path.dirname(__file__), "..", ".streamlit", "secrets.toml"),
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    secrets_path = path
                    break

        if secrets_path and os.path.exists(secrets_path):
            try:
                import toml
                secrets = toml.load(secrets_path)
                supabase = secrets.get("supabase", {})
                return cls(
                    supabase_url=supabase.get("url"),
                    supabase_key=supabase.get("key"),
                )
            except ImportError:
                # Si toml no está instalado, intentar parseo manual
                pass
            except Exception:
                pass

        return cls()

    @classmethod
    def from_env(cls) -> "AgentConfig":
        """
        Carga configuración desde variables de entorno.

        Variables soportadas:
        - SUPABASE_URL
        - SUPABASE_KEY
        - AGENT_VERBOSE (true/false)
        - PDF_DIRECTORY

        Returns:
            AgentConfig con valores de entorno
        """
        return cls(
            supabase_url=os.environ.get("SUPABASE_URL"),
            supabase_key=os.environ.get("SUPABASE_KEY"),
            verbose=os.environ.get("AGENT_VERBOSE", "").lower() == "true",
            pdf_directory=os.environ.get("PDF_DIRECTORY", "PDFS"),
        )

    @classmethod
    def from_dict(cls, data: dict) -> "AgentConfig":
        """
        Carga configuración desde diccionario.

        Args:
            data: Dict con valores de configuración

        Returns:
            AgentConfig
        """
        return cls(
            supabase_url=data.get("supabase_url"),
            supabase_key=data.get("supabase_key"),
            verbose=data.get("verbose", False),
            log_file=data.get("log_file"),
            pdf_directory=data.get("pdf_directory", "PDFS"),
        )

    def get_supabase_client(self) -> Any:
        """
        Crea y retorna un cliente de Supabase.

        Returns:
            Cliente de Supabase

        Raises:
            ValueError: Si URL o key no están configurados
            ImportError: Si supabase no está instalado
        """
        if not self.supabase_url or not self.supabase_key:
            raise ValueError(
                "Supabase URL y key son requeridos. "
                "Configura via secrets.toml o variables de entorno."
            )

        try:
            from supabase import create_client
            return create_client(self.supabase_url, self.supabase_key)
        except ImportError:
            raise ImportError(
                "El paquete 'supabase' no está instalado. "
                "Ejecuta: pip install supabase"
            )

    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        Obtiene información de una tabla.

        Args:
            table_name: Nombre de la tabla

        Returns:
            Dict con pk, fk, description o None si no existe
        """
        return self.tables.get(table_name)

    def get_all_tables(self) -> list:
        """Retorna lista de nombres de tablas."""
        return list(self.tables.keys())

    def get_child_tables(self, parent_table: str) -> list:
        """
        Obtiene tablas hijas de una tabla padre.

        Args:
            parent_table: Nombre de la tabla padre

        Returns:
            Lista de nombres de tablas hijas
        """
        children = []
        for table_name, info in self.tables.items():
            for fk_col, fk_table in info.get("fk", []):
                if fk_table == parent_table:
                    children.append(table_name)
                    break
        return children

    def validate(self) -> tuple:
        """
        Valida la configuración.

        Returns:
            Tupla (is_valid: bool, errors: list)
        """
        errors = []

        if not self.supabase_url:
            errors.append("supabase_url no está configurado")
        if not self.supabase_key:
            errors.append("supabase_key no está configurado")

        return (len(errors) == 0, errors)
