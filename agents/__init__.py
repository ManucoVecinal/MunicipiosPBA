# -*- coding: utf-8 -*-
"""
Agentes Python para MunicipiosPBA.

Tres agentes especializados para asistir desarrollo, debugging y automatizacion:
- SupabaseAgent: Operaciones de base de datos y diagnosticos
- PipelineAgent: Testing y debugging del pipeline de procesamiento de PDFs
- StreamlitAgent: Generacion de codigo y testing de UI
"""

from .base import BaseAgent
from .config import AgentConfig
from .supabase_agent import SupabaseAgent
from .pipeline_agent import PipelineAgent
from .streamlit_agent import StreamlitAgent

__all__ = [
    "BaseAgent",
    "AgentConfig",
    "SupabaseAgent",
    "PipelineAgent",
    "StreamlitAgent",
]

__version__ = "1.0.0"
