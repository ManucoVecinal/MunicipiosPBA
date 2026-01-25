Single-shot LLM ingest

Requisitos
- Python 3.11+
- Variables de entorno:
  - OPENAI_API_KEY
  - SUPABASE_URL
  - SUPABASE_KEY
  - OPENAI_MODEL (opcional, default: gpt-4.1-mini)

Instalacion
pip install -r requirements.txt

Uso
python run_single_shot_ingest.py --pdf "C:\\ruta\\archivo.pdf" --id_municipio "06007" --doc_nombre "Rendicion Q1 2025" --doc_tipo "Rendicion" --periodo "Q1 2025"

Notas
- El modelo devuelve JSON estricto con arrays por tabla; el script hace upsert en Supabase.
- bd_jurisdiccion y bd_programas salen de la misma tabla ("Evolucion de Gastos por Programa").
- bd_metas se vincula con bd_programas por Prog_Codigo y Juri_Codigo.
- Metas no mapeadas se agregan a warnings y no se insertan (o se insertan en METAS_STAGING_TABLE si existe).
