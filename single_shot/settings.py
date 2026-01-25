from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    openai_api_key: str
    openai_model: str
    supabase_url: str
    supabase_key: str
    max_retries: int
    retry_sleep_sec: float
    metas_staging_table: str | None


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip(),
        supabase_url=os.getenv("SUPABASE_URL", "").strip(),
        supabase_key=os.getenv("SUPABASE_KEY", "").strip(),
        max_retries=int(os.getenv("INGEST_MAX_RETRIES", "2")),
        retry_sleep_sec=float(os.getenv("INGEST_RETRY_SLEEP_SEC", "2.5")),
        metas_staging_table=os.getenv("METAS_STAGING_TABLE", "").strip() or None,
    )
