# -*- coding: utf-8 -*-
"""
Clase base para todos los agentes.
Proporciona utilidades comunes: logging, timing, serialización.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from datetime import datetime
import logging
import json


class BaseAgent(ABC):
    """Clase base abstracta para todos los agentes."""

    def __init__(self, name: str, verbose: bool = False):
        """
        Inicializa el agente base.

        Args:
            name: Nombre del agente (para logging)
            verbose: Si True, muestra logs de nivel DEBUG
        """
        self.name = name
        self.verbose = verbose
        self.logger = self._setup_logger()
        self._start_time: Optional[datetime] = None

    def _setup_logger(self) -> logging.Logger:
        """Configura el logger para el agente."""
        logger = logging.getLogger(f"agent.{self.name}")
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                f"[%(asctime)s] [{self.name}] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def log_info(self, message: str) -> None:
        """Log de nivel INFO."""
        self.logger.info(message)

    def log_warning(self, message: str) -> None:
        """Log de nivel WARNING."""
        self.logger.warning(message)

    def log_error(self, message: str) -> None:
        """Log de nivel ERROR."""
        self.logger.error(message)

    def log_debug(self, message: str) -> None:
        """Log de nivel DEBUG (solo si verbose=True)."""
        self.logger.debug(message)

    def start_timer(self) -> None:
        """Inicia el timer de ejecución."""
        self._start_time = datetime.now()

    def stop_timer(self) -> float:
        """
        Detiene el timer y retorna milisegundos transcurridos.

        Returns:
            Milisegundos desde start_timer(), o 0 si no se inició.
        """
        if self._start_time is None:
            return 0.0
        elapsed = (datetime.now() - self._start_time).total_seconds() * 1000
        self._start_time = None
        return elapsed

    def to_json(self, data: Any, indent: int = 2) -> str:
        """
        Convierte datos a JSON string.

        Args:
            data: Datos a serializar
            indent: Espacios de indentación

        Returns:
            String JSON formateado
        """
        return json.dumps(data, indent=indent, default=str, ensure_ascii=False)

    def from_json(self, json_str: str) -> Any:
        """
        Parsea JSON string a datos Python.

        Args:
            json_str: String JSON

        Returns:
            Datos parseados
        """
        return json.loads(json_str)

    def format_table(self, headers: list, rows: list, col_widths: list = None) -> str:
        """
        Formatea datos como tabla ASCII.

        Args:
            headers: Lista de encabezados
            rows: Lista de filas (cada fila es una lista)
            col_widths: Anchos de columna (opcional)

        Returns:
            String con tabla formateada
        """
        if col_widths is None:
            col_widths = []
            for i, h in enumerate(headers):
                max_width = len(str(h))
                for row in rows:
                    if i < len(row):
                        max_width = max(max_width, len(str(row[i])))
                col_widths.append(max_width + 2)

        lines = []
        separator = "+" + "+".join("-" * w for w in col_widths) + "+"
        lines.append(separator)

        header_line = "|"
        for i, h in enumerate(headers):
            header_line += f" {str(h):<{col_widths[i]-2}} |"
        lines.append(header_line)
        lines.append(separator)

        for row in rows:
            row_line = "|"
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    row_line += f" {str(cell):<{col_widths[i]-2}} |"
            lines.append(row_line)

        lines.append(separator)
        return "\n".join(lines)

    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """
        Verifica el estado del agente.

        Returns:
            Dict con información de estado:
            {
                "status": "ok" | "error",
                "message": str,
                "details": dict (opcional)
            }
        """
        pass
