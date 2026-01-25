LLM-assisted ETL para PDF de Rendicion de Cuentas

Estructura
- ingest/: pipeline de extraccion + carga
- ingest/schemas/: JSON Schemas para Structured Outputs
- logs/: salida de logs JSONL (se crea al correr)

Requisitos
- Python 3.11+
- Variables de entorno:
  - OPENAI_API_KEY
  - SUPABASE_URL
  - SUPABASE_KEY
  - OPENAI_MODEL (opcional, default: gpt-4.1-mini)
  - INGEST_MAX_RETRIES (opcional)
  - INGEST_RETRY_SLEEP_SEC (opcional)
  - METAS_STAGING_TABLE (opcional)

Instalacion (ejemplo)
pip install -r requirements.txt

Ejecucion
python -m ingest.run_ingest --pdf "C:\\ruta\\archivo.pdf" --municipio "San Isidro" --periodo "Q3 2025" --tipo "Rendicion"

Notas
- BD_Metas espera un campo JSONB llamado Meta_Valores. Ajusta el nombre si tu tabla usa otro.
- El router intenta detectar secciones por keywords; si no las encuentra, usa el PDF completo.
- Metas que no puedan mapearse a un programa se loguean y no se insertan (o se insertan en METAS_STAGING_TABLE).
- El mapping de metas usa Prog_Codigo o Juri_Codigo + Prog_Codigo si existe.
