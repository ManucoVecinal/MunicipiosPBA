# -*- coding: utf-8 -*-
"""
PipelineAgent: Agente para testing y debugging del pipeline de PDFs.

Funciones principales:
- Testing de parsers individuales
- Extracción y debug de texto de PDFs
- Validación de outputs
- Generación de datos de prueba
- Profiling de performance
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum
import time
import random

from .base import BaseAgent


class PipelineStep(Enum):
    """Pasos del pipeline."""
    VALIDATE = "validate"
    EXTRACT_TEXT = "extract_text"
    PARSE_RECURSOS = "parse_recursos"
    PARSE_GASTOS = "parse_gastos"
    PARSE_PROGRAMAS = "parse_programas"
    PARSE_MOVIMIENTOS = "parse_movimientos"
    PARSE_CUENTAS = "parse_cuentas"
    PARSE_SITPAT = "parse_sitpat"
    PARSE_METAS = "parse_metas"


@dataclass
class ParserResult:
    """Resultado de ejecutar un parser."""
    parser_name: str
    rows: List[Dict[str, Any]]
    warnings: List[str]
    errors: List[str]
    execution_time_ms: float

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


@dataclass
class ValidationResult:
    """Resultado de validación."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    schema_match_percentage: float


@dataclass
class ProfileResult:
    """Resultado de profiling."""
    step: str
    execution_time_ms: float
    iterations: int
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float


@dataclass
class PipelineContext:
    """Contexto que pasa a través del pipeline."""
    pdf_bytes: Optional[bytes] = None
    text_content: Optional[str] = None
    parsed_data: Dict[str, ParserResult] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class PipelineAgent(BaseAgent):
    """Agente para testing y debugging del pipeline de PDFs."""

    # Schemas esperados para validación
    EXPECTED_SCHEMAS = {
        "recursos": [
            "Rec_Tipo", "Rec_Nombre", "Rec_Categoria",
            "Rec_Vigente", "Rec_Devengado", "Rec_Percibido",
        ],
        "gastos": [
            "Gasto_Categoria", "Gasto_Objeto",
            "Gasto_Vigente", "Gasto_Preventivo", "Gasto_Compromiso",
            "Gasto_Devengado", "Gasto_Pagado",
        ],
        "programas": [
            "Juri_Codigo", "Juri_Nombre", "programas",
        ],
        "movimientos": [
            "MovTes_TipoResumido", "MovTes_Tipo", "MovTes_Importe",
        ],
        "cuentas": [
            "Cuenta_Codigo", "Cuenta_Nombre", "Cuenta_Importe",
        ],
        "sitpat": [
            "SitPat_Tipo", "SitPat_Codigo", "SitPat_Nombre", "SitPat_Saldo",
        ],
        "metas": [
            "Juri_Codigo", "Prog_Codigo", "Meta_Nombre",
            "Meta_Unidad", "Meta_Anual", "Meta_Parcial", "Meta_Ejecutado",
        ],
    }

    def __init__(self, verbose: bool = False):
        """
        Inicializa el PipelineAgent.

        Args:
            verbose: Modo verbose para logging
        """
        super().__init__(name="pipeline", verbose=verbose)
        self._parsers: Dict[str, Callable] = {}
        self._parsers_loaded = False

    def _load_parsers(self) -> None:
        """Carga los parsers del proyecto (lazy loading)."""
        if self._parsers_loaded:
            return

        try:
            from pipeline.parsers.recursos import parse_recursos_from_text
            from pipeline.parsers.gastos import parse_gastos_objeto_from_text
            from pipeline.parsers.programas import parse_programas_from_text
            from pipeline.parsers.movimientos import parse_movimientos_from_text
            from pipeline.parsers.cuentas import parse_cuentas_from_text
            from pipeline.parsers.sitpat import parse_sitpat_from_text
            from pipeline.parsers.metas import parse_metas_from_text

            self._parsers = {
                "recursos": parse_recursos_from_text,
                "gastos": parse_gastos_objeto_from_text,
                "programas": parse_programas_from_text,
                "movimientos": parse_movimientos_from_text,
                "cuentas": parse_cuentas_from_text,
                "sitpat": parse_sitpat_from_text,
                "metas": parse_metas_from_text,
            }
            self._parsers_loaded = True
            self.log_debug("Parsers cargados exitosamente")
        except ImportError as e:
            self.log_error(f"Error importando parsers: {e}")
            raise

    def health_check(self) -> Dict[str, Any]:
        """
        Verifica que los parsers estén disponibles.

        Returns:
            Dict con status
        """
        try:
            self._load_parsers()
            return {
                "status": "ok",
                "message": f"{len(self._parsers)} parsers disponibles",
                "details": {"parsers": list(self._parsers.keys())},
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error cargando parsers: {e}",
                "details": {},
            }

    def get_available_parsers(self) -> List[str]:
        """Retorna lista de parsers disponibles."""
        self._load_parsers()
        return list(self._parsers.keys())

    # === PDF Processing ===

    def load_pdf(self, pdf_path: str) -> bytes:
        """
        Carga un PDF desde archivo.

        Args:
            pdf_path: Ruta al archivo PDF

        Returns:
            Bytes del PDF
        """
        self.log_debug(f"Cargando PDF: {pdf_path}")
        with open(pdf_path, "rb") as f:
            return f.read()

    def extract_text(self, pdf_bytes: bytes) -> str:
        """
        Extrae texto de un PDF.

        Args:
            pdf_bytes: Bytes del PDF

        Returns:
            Texto extraído
        """
        try:
            import pdfplumber
            import io

            self.start_timer()
            text_parts = []

            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)

            elapsed = self.stop_timer()
            full_text = "\n".join(text_parts)
            self.log_debug(f"Texto extraido: {len(full_text)} chars en {elapsed:.0f}ms")
            return full_text

        except ImportError:
            raise ImportError("pdfplumber no instalado. Ejecuta: pip install pdfplumber")

    def extract_text_by_page(
        self,
        pdf_bytes: bytes,
        page_range: Tuple[int, int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extrae texto página por página.

        Args:
            pdf_bytes: Bytes del PDF
            page_range: Tupla (inicio, fin) opcional

        Returns:
            Lista de dicts con {page_num, text, char_count}
        """
        try:
            import pdfplumber
            import io

            results = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                total_pages = len(pdf.pages)

                if page_range:
                    start, end = page_range
                    start = max(0, start - 1)  # 1-indexed a 0-indexed
                    end = min(total_pages, end)
                else:
                    start, end = 0, total_pages

                for i in range(start, end):
                    page = pdf.pages[i]
                    text = page.extract_text() or ""
                    results.append({
                        "page_num": i + 1,
                        "text": text,
                        "char_count": len(text),
                    })

            return results
        except ImportError:
            raise ImportError("pdfplumber no instalado")

    # === Parser Testing ===

    def test_parser(self, parser_name: str, text: str) -> ParserResult:
        """
        Ejecuta un parser sobre texto.

        Args:
            parser_name: Nombre del parser
            text: Texto a parsear

        Returns:
            ParserResult con resultados
        """
        self._load_parsers()

        if parser_name not in self._parsers:
            raise ValueError(
                f"Parser '{parser_name}' no existe. "
                f"Disponibles: {list(self._parsers.keys())}"
            )

        parser_func = self._parsers[parser_name]
        self.log_debug(f"Ejecutando parser: {parser_name}")

        self.start_timer()
        try:
            result = parser_func(text)
            elapsed = self.stop_timer()

            # Los parsers retornan (rows, warnings) o similar
            if isinstance(result, tuple):
                rows, warnings = result[0], result[1] if len(result) > 1 else []
            else:
                rows, warnings = result, []

            # Normalizar rows
            if rows is None:
                rows = []

            return ParserResult(
                parser_name=parser_name,
                rows=rows,
                warnings=warnings if isinstance(warnings, list) else [warnings],
                errors=[],
                execution_time_ms=elapsed,
            )

        except Exception as e:
            elapsed = self.stop_timer()
            self.log_error(f"Error en parser {parser_name}: {e}")
            return ParserResult(
                parser_name=parser_name,
                rows=[],
                warnings=[],
                errors=[str(e)],
                execution_time_ms=elapsed,
            )

    def test_all_parsers(self, text: str) -> Dict[str, ParserResult]:
        """
        Ejecuta todos los parsers sobre texto.

        Args:
            text: Texto a parsear

        Returns:
            Dict con resultado por parser
        """
        self._load_parsers()
        results = {}

        for parser_name in self._parsers:
            self.log_info(f"Testing parser: {parser_name}")
            results[parser_name] = self.test_parser(parser_name, text)

        return results

    def run_parser_on_pdf(self, pdf_path: str, parser_name: str) -> ParserResult:
        """
        Ejecuta un parser directamente sobre un PDF.

        Args:
            pdf_path: Ruta al PDF
            parser_name: Nombre del parser

        Returns:
            ParserResult
        """
        pdf_bytes = self.load_pdf(pdf_path)
        text = self.extract_text(pdf_bytes)
        return self.test_parser(parser_name, text)

    def run_all_parsers_on_pdf(self, pdf_path: str) -> Dict[str, ParserResult]:
        """
        Ejecuta todos los parsers sobre un PDF.

        Args:
            pdf_path: Ruta al PDF

        Returns:
            Dict con resultados
        """
        pdf_bytes = self.load_pdf(pdf_path)
        text = self.extract_text(pdf_bytes)
        return self.test_all_parsers(text)

    # === Validation ===

    def validate_parser_output(
        self,
        parser_name: str,
        rows: List[Dict],
    ) -> ValidationResult:
        """
        Valida output de un parser contra schema esperado.

        Args:
            parser_name: Nombre del parser
            rows: Filas a validar

        Returns:
            ValidationResult
        """
        expected = self.EXPECTED_SCHEMAS.get(parser_name, [])
        if not expected:
            return ValidationResult(
                is_valid=True,
                errors=[],
                warnings=[f"No hay schema definido para '{parser_name}'"],
                schema_match_percentage=100.0,
            )

        errors = []
        warnings = []

        if not rows:
            return ValidationResult(
                is_valid=True,
                errors=[],
                warnings=["Sin filas para validar"],
                schema_match_percentage=0.0,
            )

        # Verificar campos en primera fila
        first_row_keys = set(rows[0].keys())
        expected_set = set(expected)

        missing = expected_set - first_row_keys
        extra = first_row_keys - expected_set

        if missing:
            errors.append(f"Campos faltantes: {missing}")
        if extra:
            warnings.append(f"Campos extra (OK): {extra}")

        # Calcular porcentaje de match
        match_count = len(expected_set & first_row_keys)
        match_pct = (match_count / len(expected_set)) * 100 if expected_set else 100.0

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            schema_match_percentage=round(match_pct, 1),
        )

    # === Debugging ===

    def find_section(self, text: str, section_name: str) -> Dict[str, Any]:
        """
        Busca una sección específica en el texto.

        Args:
            text: Texto completo
            section_name: Nombre de la sección a buscar

        Returns:
            Dict con {found, start_pos, preview}
        """
        import re

        # Normalizar búsqueda
        pattern = re.compile(re.escape(section_name), re.IGNORECASE)
        match = pattern.search(text)

        if match:
            start = match.start()
            # Extraer contexto (500 chars después del match)
            preview = text[start:start + 500]
            return {
                "found": True,
                "start_pos": start,
                "preview": preview,
                "line_approx": text[:start].count("\n") + 1,
            }

        return {
            "found": False,
            "start_pos": -1,
            "preview": "",
            "suggestions": self._suggest_sections(text, section_name),
        }

    def _suggest_sections(self, text: str, query: str) -> List[str]:
        """Sugiere secciones similares."""
        import re

        # Buscar líneas que parezcan títulos de sección
        lines = text.split("\n")
        suggestions = []

        query_lower = query.lower()
        for line in lines:
            line_clean = line.strip().lower()
            if len(line_clean) > 10 and len(line_clean) < 100:
                # Buscar similitud básica
                if any(word in line_clean for word in query_lower.split()):
                    suggestions.append(line.strip()[:80])

        return suggestions[:5]

    def debug_extraction(
        self,
        pdf_path: str,
        page_range: Tuple[int, int] = None,
    ) -> Dict[str, Any]:
        """
        Debug detallado de extracción de texto.

        Args:
            pdf_path: Ruta al PDF
            page_range: Páginas a analizar

        Returns:
            Dict con información de debug
        """
        pdf_bytes = self.load_pdf(pdf_path)
        pages = self.extract_text_by_page(pdf_bytes, page_range)

        total_chars = sum(p["char_count"] for p in pages)
        empty_pages = [p["page_num"] for p in pages if p["char_count"] < 50]

        return {
            "pdf_path": pdf_path,
            "total_pages": len(pages),
            "total_chars": total_chars,
            "avg_chars_per_page": total_chars // len(pages) if pages else 0,
            "empty_pages": empty_pages,
            "pages": pages,
        }

    # === Sample Data Generation ===

    def generate_sample_recursos(self, count: int = 5) -> List[Dict]:
        """Genera datos de prueba para recursos."""
        tipos = ["Presupuestarios", "Extrapresupuestarios"]
        categorias = [
            "Corrientes", "De Capital",
            "Ingresos de libre disponibilidad", "Ingresos con afectacion especifica",
        ]
        nombres = [
            "Impuestos sobre los ingresos",
            "Impuestos sobre el patrimonio",
            "Tasas municipales",
            "Contribuciones de mejoras",
            "Venta de bienes",
            "Transferencias corrientes",
        ]

        rows = []
        for i in range(count):
            vigente = random.uniform(1000000, 50000000)
            devengado = vigente * random.uniform(0.7, 1.0)
            percibido = devengado * random.uniform(0.8, 1.0)

            rows.append({
                "Rec_Tipo": random.choice(tipos),
                "Rec_Nombre": random.choice(nombres),
                "Rec_Categoria": random.choice(categorias),
                "Rec_Vigente": round(vigente, 2),
                "Rec_Devengado": round(devengado, 2),
                "Rec_Percibido": round(percibido, 2),
            })
        return rows

    def generate_sample_gastos(self, count: int = 5) -> List[Dict]:
        """Genera datos de prueba para gastos."""
        objetos = [
            "1 - Gastos en Personal",
            "2 - Bienes de Consumo",
            "3 - Servicios No Personales",
            "4 - Bienes de Uso",
            "5 - Transferencias",
        ]
        categorias = ["Corriente", "De Capital"]

        rows = []
        for i in range(count):
            vigente = random.uniform(500000, 30000000)
            preventivo = vigente * random.uniform(0.9, 1.0)
            compromiso = preventivo * random.uniform(0.85, 1.0)
            devengado = compromiso * random.uniform(0.8, 1.0)
            pagado = devengado * random.uniform(0.7, 1.0)

            rows.append({
                "Gasto_Objeto": random.choice(objetos),
                "Gasto_Categoria": random.choice(categorias),
                "Gasto_Vigente": round(vigente, 2),
                "Gasto_Preventivo": round(preventivo, 2),
                "Gasto_Compromiso": round(compromiso, 2),
                "Gasto_Devengado": round(devengado, 2),
                "Gasto_Pagado": round(pagado, 2),
            })
        return rows

    def generate_sample_data(self, parser_name: str, count: int = 5) -> List[Dict]:
        """
        Genera datos de prueba para cualquier parser.

        Args:
            parser_name: Nombre del parser
            count: Cantidad de registros

        Returns:
            Lista de dicts con datos de prueba
        """
        generators = {
            "recursos": self.generate_sample_recursos,
            "gastos": self.generate_sample_gastos,
        }

        if parser_name in generators:
            return generators[parser_name](count)

        # Generar datos genéricos basados en schema
        schema = self.EXPECTED_SCHEMAS.get(parser_name, [])
        if not schema:
            return []

        rows = []
        for i in range(count):
            row = {}
            for field in schema:
                if "importe" in field.lower() or "saldo" in field.lower():
                    row[field] = round(random.uniform(10000, 1000000), 2)
                elif "codigo" in field.lower():
                    row[field] = f"{random.randint(1, 99):02d}"
                elif "nombre" in field.lower():
                    row[field] = f"Item de prueba {i+1}"
                else:
                    row[field] = f"valor_{i+1}"
            rows.append(row)
        return rows

    # === Profiling ===

    def profile_parser(
        self,
        parser_name: str,
        text: str,
        iterations: int = 10,
    ) -> ProfileResult:
        """
        Mide performance de un parser.

        Args:
            parser_name: Parser a medir
            text: Texto de entrada
            iterations: Número de iteraciones

        Returns:
            ProfileResult con métricas
        """
        self._load_parsers()

        if parser_name not in self._parsers:
            raise ValueError(f"Parser '{parser_name}' no existe")

        parser_func = self._parsers[parser_name]
        times = []

        for _ in range(iterations):
            start = time.perf_counter()
            try:
                parser_func(text)
            except Exception:
                pass
            end = time.perf_counter()
            times.append((end - start) * 1000)

        return ProfileResult(
            step=parser_name,
            execution_time_ms=sum(times),
            iterations=iterations,
            avg_time_ms=sum(times) / len(times),
            min_time_ms=min(times),
            max_time_ms=max(times),
        )

    def benchmark_all_parsers(self, text: str, iterations: int = 5) -> Dict[str, ProfileResult]:
        """
        Benchmark de todos los parsers.

        Args:
            text: Texto de entrada
            iterations: Iteraciones por parser

        Returns:
            Dict con ProfileResult por parser
        """
        results = {}
        for parser_name in self.get_available_parsers():
            self.log_info(f"Benchmarking: {parser_name}")
            results[parser_name] = self.profile_parser(parser_name, text, iterations)
        return results

    # === Reporting ===

    def generate_parsing_report(self, results: Dict[str, ParserResult]) -> str:
        """
        Genera reporte de resultados de parsing.

        Args:
            results: Dict de ParserResult

        Returns:
            String con reporte formateado
        """
        lines = ["=== REPORTE DE PARSING ===\n"]

        total_rows = 0
        total_warnings = 0
        total_errors = 0

        for parser_name, result in results.items():
            status = "OK" if result.success else "ERROR"
            lines.append(f"\n[{parser_name.upper()}] - {status}")
            lines.append(f"  Filas: {len(result.rows)}")
            lines.append(f"  Tiempo: {result.execution_time_ms:.0f}ms")

            if result.warnings:
                lines.append(f"  Warnings: {len(result.warnings)}")
            if result.errors:
                lines.append(f"  Errors: {result.errors}")

            total_rows += len(result.rows)
            total_warnings += len(result.warnings)
            total_errors += len(result.errors)

        lines.append(f"\n=== TOTALES ===")
        lines.append(f"Filas totales: {total_rows}")
        lines.append(f"Warnings: {total_warnings}")
        lines.append(f"Errors: {total_errors}")

        return "\n".join(lines)
