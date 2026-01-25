# -*- coding: utf-8 -*-
"""
SupabaseAgent: Agente para operaciones de base de datos y diagnósticos.

Funciones principales:
- Inspección de tablas y schemas
- Diagnósticos de integridad
- Búsqueda de registros huérfanos
- Operaciones batch (limpieza, exports)
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from enum import Enum
import json

from .base import BaseAgent
from .config import AgentConfig


class DiagnosticType(Enum):
    """Tipos de diagnóstico disponibles."""
    ORPHAN_RECORDS = "orphan_records"
    MISSING_DATA = "missing_data"
    INTEGRITY = "integrity"
    DUPLICATES = "duplicates"
    CONNECTION = "connection"


@dataclass
class TableInfo:
    """Información de una tabla."""
    name: str
    pk_column: str
    fk_columns: List[tuple]
    row_count: int
    description: str


@dataclass
class DiagnosticResult:
    """Resultado de un diagnóstico."""
    diagnostic_type: DiagnosticType
    table: str
    issues: List[Dict[str, Any]]
    severity: str  # "info", "warning", "error"
    recommendation: str
    count: int = 0


class SupabaseAgent(BaseAgent):
    """Agente para operaciones de Supabase y diagnósticos de BD."""

    def __init__(
        self,
        supabase_client=None,
        config: AgentConfig = None,
        verbose: bool = False,
    ):
        """
        Inicializa el SupabaseAgent.

        Args:
            supabase_client: Cliente de Supabase (opcional)
            config: Configuración del agente (opcional)
            verbose: Modo verbose para logging
        """
        super().__init__(name="supabase", verbose=verbose)
        self.config = config or AgentConfig.from_streamlit_secrets()
        self._client = supabase_client
        self._connected = False

    @property
    def client(self):
        """Obtiene el cliente de Supabase (lazy loading)."""
        if self._client is None:
            self._client = self.config.get_supabase_client()
            self._connected = True
        return self._client

    def health_check(self) -> Dict[str, Any]:
        """
        Verifica la conexión a Supabase.

        Returns:
            Dict con status de conexión
        """
        self.start_timer()
        try:
            # Intentar una consulta simple
            res = self.client.table("bd_municipios").select("ID_Municipio").limit(1).execute()
            elapsed = self.stop_timer()

            return {
                "status": "ok",
                "message": "Conexion a Supabase exitosa",
                "details": {
                    "response_time_ms": round(elapsed, 2),
                    "url": self.config.supabase_url[:30] + "..." if self.config.supabase_url else None,
                },
            }
        except Exception as e:
            elapsed = self.stop_timer()
            return {
                "status": "error",
                "message": f"Error de conexion: {str(e)}",
                "details": {"response_time_ms": round(elapsed, 2)},
            }

    # === Inspección de Tablas ===

    def inspect_table(self, table_name: str) -> TableInfo:
        """
        Obtiene información detallada de una tabla.

        Args:
            table_name: Nombre de la tabla

        Returns:
            TableInfo con detalles de la tabla
        """
        table_config = self.config.get_table_info(table_name)
        if not table_config:
            raise ValueError(f"Tabla '{table_name}' no esta configurada")

        # Contar registros
        self.log_debug(f"Inspeccionando tabla: {table_name}")
        try:
            res = self.client.table(table_name).select("*", count="exact").limit(0).execute()
            row_count = res.count if hasattr(res, "count") else 0
        except Exception as e:
            self.log_warning(f"No se pudo contar registros en {table_name}: {e}")
            row_count = -1

        return TableInfo(
            name=table_name,
            pk_column=table_config["pk"],
            fk_columns=table_config.get("fk", []),
            row_count=row_count,
            description=table_config.get("description", ""),
        )

    def inspect_all_tables(self) -> List[TableInfo]:
        """
        Inspecciona todas las tablas del proyecto.

        Returns:
            Lista de TableInfo para cada tabla
        """
        results = []
        for table_name in self.config.get_all_tables():
            try:
                info = self.inspect_table(table_name)
                results.append(info)
                self.log_info(f"{table_name}: {info.row_count} registros")
            except Exception as e:
                self.log_error(f"Error inspeccionando {table_name}: {e}")
        return results

    def get_table_summary(self) -> str:
        """
        Genera un resumen de todas las tablas.

        Returns:
            String con tabla formateada
        """
        tables = self.inspect_all_tables()
        headers = ["Tabla", "PK", "Registros", "Descripcion"]
        rows = [
            [t.name, t.pk_column, t.row_count, t.description[:30]]
            for t in tables
        ]
        return self.format_table(headers, rows)

    # === Diagnósticos ===

    def find_orphan_records(
        self,
        child_table: str,
        parent_table: str,
        fk_column: str,
    ) -> DiagnosticResult:
        """
        Busca registros huérfanos (sin padre).

        Args:
            child_table: Tabla hija
            parent_table: Tabla padre
            fk_column: Columna FK en la tabla hija

        Returns:
            DiagnosticResult con registros huérfanos
        """
        self.log_debug(f"Buscando huerfanos: {child_table}.{fk_column} -> {parent_table}")

        parent_config = self.config.get_table_info(parent_table)
        if not parent_config:
            raise ValueError(f"Tabla padre '{parent_table}' no configurada")

        parent_pk = parent_config["pk"]

        # Obtener todos los IDs del padre
        parent_res = self.client.table(parent_table).select(parent_pk).execute()
        parent_ids = set()
        if hasattr(parent_res, "data") and parent_res.data:
            parent_ids = {str(row[parent_pk]) for row in parent_res.data}

        # Obtener registros de la tabla hija
        child_config = self.config.get_table_info(child_table)
        child_pk = child_config["pk"]
        child_res = self.client.table(child_table).select(f"{child_pk},{fk_column}").execute()

        orphans = []
        if hasattr(child_res, "data") and child_res.data:
            for row in child_res.data:
                fk_value = row.get(fk_column)
                if fk_value and str(fk_value) not in parent_ids:
                    orphans.append({
                        child_pk: row[child_pk],
                        fk_column: fk_value,
                    })

        severity = "error" if len(orphans) > 0 else "info"
        recommendation = (
            f"Eliminar {len(orphans)} registros huerfanos de {child_table}"
            if orphans else "Sin registros huerfanos"
        )

        return DiagnosticResult(
            diagnostic_type=DiagnosticType.ORPHAN_RECORDS,
            table=child_table,
            issues=orphans,
            severity=severity,
            recommendation=recommendation,
            count=len(orphans),
        )

    def find_duplicates(
        self,
        table_name: str,
        columns: List[str],
    ) -> DiagnosticResult:
        """
        Busca registros duplicados basado en columnas.

        Args:
            table_name: Nombre de la tabla
            columns: Columnas a verificar unicidad

        Returns:
            DiagnosticResult con duplicados encontrados
        """
        self.log_debug(f"Buscando duplicados en {table_name} por {columns}")

        table_config = self.config.get_table_info(table_name)
        pk = table_config["pk"]

        # Obtener todos los registros
        select_cols = f"{pk}," + ",".join(columns)
        res = self.client.table(table_name).select(select_cols).execute()

        # Buscar duplicados
        seen = {}
        duplicates = []

        if hasattr(res, "data") and res.data:
            for row in res.data:
                key = tuple(str(row.get(col, "")) for col in columns)
                if key in seen:
                    duplicates.append({
                        pk: row[pk],
                        "duplicate_of": seen[key],
                        "key": {col: row.get(col) for col in columns},
                    })
                else:
                    seen[key] = row[pk]

        severity = "warning" if len(duplicates) > 0 else "info"
        return DiagnosticResult(
            diagnostic_type=DiagnosticType.DUPLICATES,
            table=table_name,
            issues=duplicates,
            severity=severity,
            recommendation=f"Revisar {len(duplicates)} duplicados" if duplicates else "Sin duplicados",
            count=len(duplicates),
        )

    def validate_data_integrity(self, table_name: str = None) -> List[DiagnosticResult]:
        """
        Valida integridad referencial.

        Args:
            table_name: Tabla específica o None para todas

        Returns:
            Lista de DiagnosticResult
        """
        results = []
        tables = [table_name] if table_name else self.config.get_all_tables()

        for tbl in tables:
            table_config = self.config.get_table_info(tbl)
            if not table_config:
                continue

            # Verificar cada FK
            for fk_col, parent_tbl in table_config.get("fk", []):
                try:
                    result = self.find_orphan_records(tbl, parent_tbl, fk_col)
                    results.append(result)
                except Exception as e:
                    self.log_error(f"Error validando {tbl}.{fk_col}: {e}")

        return results

    def run_all_diagnostics(self) -> Dict[str, List[DiagnosticResult]]:
        """
        Ejecuta todos los diagnósticos disponibles.

        Returns:
            Dict con resultados por categoría
        """
        self.log_info("Ejecutando diagnosticos completos...")

        results = {
            "connection": [],
            "integrity": [],
            "summary": [],
        }

        # Test de conexión
        health = self.health_check()
        results["connection"].append(DiagnosticResult(
            diagnostic_type=DiagnosticType.CONNECTION,
            table="",
            issues=[health] if health["status"] == "error" else [],
            severity="error" if health["status"] == "error" else "info",
            recommendation=health["message"],
            count=0 if health["status"] == "ok" else 1,
        ))

        if health["status"] == "error":
            return results

        # Integridad referencial
        results["integrity"] = self.validate_data_integrity()

        return results

    def get_diagnostics_summary(self) -> str:
        """
        Genera resumen de diagnósticos.

        Returns:
            String formateado con resultados
        """
        diagnostics = self.run_all_diagnostics()

        lines = ["=== DIAGNOSTICOS DE BASE DE DATOS ===\n"]

        total_issues = 0
        for category, results in diagnostics.items():
            lines.append(f"\n[{category.upper()}]")
            for r in results:
                total_issues += r.count
                status = "OK" if r.count == 0 else f"ISSUES: {r.count}"
                lines.append(f"  {r.table or 'general'}: {status} - {r.recommendation}")

        lines.append(f"\n=== TOTAL ISSUES: {total_issues} ===")
        return "\n".join(lines)

    # === Operaciones de Datos ===

    def count_records_by_document(self, doc_id: str) -> Dict[str, int]:
        """
        Cuenta registros asociados a un documento.

        Args:
            doc_id: ID del documento

        Returns:
            Dict con conteo por tabla
        """
        counts = {}
        doc_tables = [
            "bd_recursos", "bd_gastos", "bd_jurisdiccion",
            "bd_situacionpatrimonial", "bd_movimientosTesoreria", "bd_cuentas",
        ]

        for table in doc_tables:
            try:
                res = self.client.table(table).select(
                    "*", count="exact"
                ).eq("ID_DocumentoCargado", doc_id).limit(0).execute()
                counts[table] = res.count if hasattr(res, "count") else 0
            except Exception as e:
                self.log_warning(f"Error contando en {table}: {e}")
                counts[table] = -1

        return counts

    def count_records_by_municipio(self, muni_id: str) -> Dict[str, int]:
        """
        Cuenta registros asociados a un municipio.

        Args:
            muni_id: ID del municipio

        Returns:
            Dict con conteo por tabla
        """
        counts = {}

        # Primero contar documentos
        try:
            res = self.client.table("BD_DocumentosCargados").select(
                "ID_DocumentoCargado", count="exact"
            ).eq("ID_Municipio", muni_id).execute()
            counts["BD_DocumentosCargados"] = res.count if hasattr(res, "count") else 0
            doc_ids = [row["ID_DocumentoCargado"] for row in (res.data or [])]
        except Exception as e:
            self.log_warning(f"Error contando documentos: {e}")
            return counts

        # Contar registros de cada documento
        doc_tables = [
            "bd_recursos", "bd_gastos", "bd_jurisdiccion",
            "bd_situacionpatrimonial", "bd_movimientosTesoreria", "bd_cuentas",
        ]

        for table in doc_tables:
            total = 0
            for doc_id in doc_ids:
                try:
                    res = self.client.table(table).select(
                        "*", count="exact"
                    ).eq("ID_DocumentoCargado", doc_id).limit(0).execute()
                    total += res.count if hasattr(res, "count") else 0
                except Exception:
                    pass
            counts[table] = total

        return counts

    def get_document_status_summary(self) -> Dict[str, int]:
        """
        Obtiene resumen de documentos por estado.

        Returns:
            Dict con conteo por estado
        """
        try:
            res = self.client.table("BD_DocumentosCargados").select("Doc_Estado").execute()
            status_counts = {}
            if hasattr(res, "data") and res.data:
                for row in res.data:
                    status = row.get("Doc_Estado", "desconocido")
                    status_counts[status] = status_counts.get(status, 0) + 1
            return status_counts
        except Exception as e:
            self.log_error(f"Error obteniendo estados: {e}")
            return {}

    # === Operaciones Batch ===

    def cleanup_orphan_records(
        self,
        table_name: str,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        Limpia registros huérfanos de una tabla.

        Args:
            table_name: Tabla a limpiar
            dry_run: Si True, solo muestra qué se borraría

        Returns:
            Dict con resultado de la operación
        """
        table_config = self.config.get_table_info(table_name)
        if not table_config:
            raise ValueError(f"Tabla '{table_name}' no configurada")

        all_orphans = []
        pk = table_config["pk"]

        # Buscar huérfanos para cada FK
        for fk_col, parent_tbl in table_config.get("fk", []):
            result = self.find_orphan_records(table_name, parent_tbl, fk_col)
            all_orphans.extend(result.issues)

        if not all_orphans:
            return {
                "status": "ok",
                "message": "Sin registros huerfanos",
                "deleted": 0,
                "dry_run": dry_run,
            }

        orphan_ids = list(set(o[pk] for o in all_orphans))

        if dry_run:
            return {
                "status": "preview",
                "message": f"Se eliminarian {len(orphan_ids)} registros",
                "ids": orphan_ids[:10],  # Solo mostrar primeros 10
                "total": len(orphan_ids),
                "dry_run": True,
            }

        # Ejecutar eliminación
        try:
            from pipeline.load_supabase import delete_rows
            deleted = delete_rows(self.client, table_name, pk, orphan_ids)
            return {
                "status": "ok",
                "message": f"Eliminados {deleted} registros",
                "deleted": deleted,
                "dry_run": False,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error eliminando: {e}",
                "deleted": 0,
                "dry_run": False,
            }

    def reset_document_data(
        self,
        doc_id: str,
        tables: List[str] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        Elimina todos los datos de un documento para reprocesamiento.

        Args:
            doc_id: ID del documento
            tables: Lista de tablas (None = todas)
            dry_run: Si True, solo muestra qué se borraría

        Returns:
            Dict con resultado
        """
        if tables is None:
            tables = [
                "bd_recursos", "bd_gastos", "bd_jurisdiccion",
                "bd_situacionpatrimonial", "bd_movimientosTesoreria", "bd_cuentas",
            ]

        counts = self.count_records_by_document(doc_id)

        if dry_run:
            return {
                "status": "preview",
                "message": f"Se eliminarian datos del documento {doc_id}",
                "counts": counts,
                "dry_run": True,
            }

        # Ejecutar eliminación
        deleted = {}
        try:
            from pipeline.load_supabase import delete_rows_by_filters
            for table in tables:
                if counts.get(table, 0) > 0:
                    n = delete_rows_by_filters(
                        self.client, table, {"ID_DocumentoCargado": doc_id}
                    )
                    deleted[table] = n
            return {
                "status": "ok",
                "message": "Datos eliminados",
                "deleted": deleted,
                "dry_run": False,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error eliminando: {e}",
                "deleted": deleted,
                "dry_run": False,
            }

    # === Export/Import ===

    def export_table_data(
        self,
        table_name: str,
        filters: Dict[str, Any] = None,
        format: str = "json",
    ) -> str:
        """
        Exporta datos de una tabla.

        Args:
            table_name: Nombre de la tabla
            filters: Filtros a aplicar (opcional)
            format: Formato de salida ("json" o "csv")

        Returns:
            String con datos exportados
        """
        query = self.client.table(table_name).select("*")

        if filters:
            for key, value in filters.items():
                if value is not None:
                    query = query.eq(key, value)

        res = query.execute()
        data = res.data if hasattr(res, "data") else []

        if format == "json":
            return self.to_json(data)
        elif format == "csv":
            if not data:
                return ""
            import csv
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            return output.getvalue()
        else:
            raise ValueError(f"Formato no soportado: {format}")
