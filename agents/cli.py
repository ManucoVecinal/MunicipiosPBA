#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CLI unificado para los agentes de MunicipiosPBA.

Uso:
    python -m agents.cli supabase health
    python -m agents.cli pipeline test archivo.pdf
    python -m agents.cli streamlit generate tab --name X --table Y
"""

import argparse
import sys
import json
from typing import List, Optional

from .config import AgentConfig
from .supabase_agent import SupabaseAgent
from .pipeline_agent import PipelineAgent
from .streamlit_agent import StreamlitAgent


def create_parser() -> argparse.ArgumentParser:
    """Crea el parser principal de argumentos."""
    parser = argparse.ArgumentParser(
        prog="agents",
        description="CLI de agentes para MunicipiosPBA",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python -m agents.cli supabase health
  python -m agents.cli supabase inspect bd_recursos
  python -m agents.cli supabase diagnose all

  python -m agents.cli pipeline test PDFS/archivo.pdf
  python -m agents.cli pipeline test PDFS/archivo.pdf --parser recursos

  python -m agents.cli streamlit generate crud --table bd_gastos
  python -m agents.cli streamlit mock --table bd_recursos --count 10
        """,
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Modo verbose (mas detalle en logs)",
    )

    subparsers = parser.add_subparsers(dest="agent", help="Agente a usar")

    # === SUPABASE AGENT ===
    supabase_parser = subparsers.add_parser(
        "supabase",
        help="Agente de base de datos Supabase",
    )
    supabase_sub = supabase_parser.add_subparsers(dest="command")

    # supabase health
    supabase_sub.add_parser("health", help="Verificar conexion a Supabase")

    # supabase inspect
    inspect_parser = supabase_sub.add_parser("inspect", help="Inspeccionar tablas")
    inspect_parser.add_argument(
        "table",
        nargs="?",
        help="Tabla a inspeccionar (omitir para todas)",
    )

    # supabase diagnose
    diagnose_parser = supabase_sub.add_parser("diagnose", help="Ejecutar diagnosticos")
    diagnose_parser.add_argument(
        "type",
        choices=["all", "orphans", "integrity"],
        help="Tipo de diagnostico",
    )
    diagnose_parser.add_argument(
        "--table",
        help="Tabla especifica (para orphans)",
    )
    diagnose_parser.add_argument(
        "--output",
        help="Archivo de salida (JSON)",
    )

    # supabase export
    export_parser = supabase_sub.add_parser("export", help="Exportar datos")
    export_parser.add_argument("table", help="Tabla a exportar")
    export_parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Formato de salida",
    )

    # supabase cleanup
    cleanup_parser = supabase_sub.add_parser("cleanup", help="Limpiar registros")
    cleanup_parser.add_argument(
        "type",
        choices=["orphans"],
        help="Tipo de limpieza",
    )
    cleanup_parser.add_argument("--table", required=True, help="Tabla")
    cleanup_parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Solo mostrar que se haria (default)",
    )
    cleanup_parser.add_argument(
        "--execute",
        action="store_true",
        help="Ejecutar la limpieza",
    )

    # supabase count
    count_parser = supabase_sub.add_parser("count", help="Contar registros")
    count_parser.add_argument(
        "--doc-id",
        help="ID de documento",
    )
    count_parser.add_argument(
        "--muni-id",
        help="ID de municipio",
    )

    # === PIPELINE AGENT ===
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Agente de pipeline de PDFs",
    )
    pipeline_sub = pipeline_parser.add_subparsers(dest="command")

    # pipeline health
    pipeline_sub.add_parser("health", help="Verificar parsers disponibles")

    # pipeline test
    test_parser = pipeline_sub.add_parser("test", help="Testear PDF")
    test_parser.add_argument("pdf_path", help="Ruta al PDF")
    test_parser.add_argument(
        "--parser",
        help="Parser especifico (omitir para todos)",
    )

    # pipeline extract-text
    extract_parser = pipeline_sub.add_parser("extract-text", help="Extraer texto")
    extract_parser.add_argument("pdf_path", help="Ruta al PDF")
    extract_parser.add_argument(
        "--pages",
        help="Rango de paginas (ej: 1-5)",
    )
    extract_parser.add_argument(
        "--output",
        help="Archivo de salida",
    )

    # pipeline debug
    debug_parser = pipeline_sub.add_parser("debug", help="Debug de extraccion")
    debug_parser.add_argument("pdf_path", help="Ruta al PDF")
    debug_parser.add_argument(
        "--section",
        help="Buscar seccion especifica",
    )

    # pipeline generate-sample
    sample_parser = pipeline_sub.add_parser("generate-sample", help="Generar datos de prueba")
    sample_parser.add_argument(
        "--type",
        choices=["recursos", "gastos", "programas", "all"],
        default="recursos",
        help="Tipo de datos",
    )
    sample_parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Cantidad de registros",
    )
    sample_parser.add_argument(
        "--output",
        help="Archivo de salida",
    )

    # pipeline benchmark
    benchmark_parser = pipeline_sub.add_parser("benchmark", help="Benchmark de parsers")
    benchmark_parser.add_argument("pdf_path", help="Ruta al PDF")
    benchmark_parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Iteraciones",
    )

    # === STREAMLIT AGENT ===
    streamlit_parser = subparsers.add_parser(
        "streamlit",
        help="Agente de UI Streamlit",
    )
    streamlit_sub = streamlit_parser.add_subparsers(dest="command")

    # streamlit health
    streamlit_sub.add_parser("health", help="Verificar agente")

    # streamlit generate
    generate_parser = streamlit_sub.add_parser("generate", help="Generar codigo")
    generate_parser.add_argument(
        "component",
        choices=["tab", "form", "editor", "delete-ui", "crud"],
        help="Tipo de componente",
    )
    generate_parser.add_argument("--table", required=True, help="Tabla")
    generate_parser.add_argument("--name", help="Nombre del componente")
    generate_parser.add_argument("--output", help="Archivo de salida")

    # streamlit mock
    mock_parser = streamlit_sub.add_parser("mock", help="Generar datos mock")
    mock_parser.add_argument(
        "--type",
        choices=["municipio", "document", "session-state"] + list(StreamlitAgent.TABLE_UI_CONFIGS.keys()),
        default="municipio",
        help="Tipo de datos",
    )
    mock_parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Cantidad",
    )
    mock_parser.add_argument("--output", help="Archivo de salida")

    # streamlit validate-session
    validate_parser = streamlit_sub.add_parser("validate-session", help="Validar session state")
    validate_parser.add_argument(
        "--state-file",
        help="Archivo JSON con session state",
    )

    # streamlit list-tables
    streamlit_sub.add_parser("list-tables", help="Listar tablas configuradas")

    return parser


def handle_supabase_command(agent: SupabaseAgent, args) -> int:
    """Maneja comandos del agente Supabase."""
    if args.command == "health":
        result = agent.health_check()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["status"] == "ok" else 1

    elif args.command == "inspect":
        if args.table:
            info = agent.inspect_table(args.table)
            print(f"Tabla: {info.name}")
            print(f"PK: {info.pk_column}")
            print(f"Registros: {info.row_count}")
            print(f"FKs: {info.fk_columns}")
            print(f"Descripcion: {info.description}")
        else:
            summary = agent.get_table_summary()
            print(summary)
        return 0

    elif args.command == "diagnose":
        if args.type == "all":
            print(agent.get_diagnostics_summary())
        elif args.type == "integrity":
            results = agent.validate_data_integrity(args.table)
            for r in results:
                print(f"[{r.severity.upper()}] {r.table}: {r.recommendation} ({r.count} issues)")
        return 0

    elif args.command == "export":
        data = agent.export_table_data(args.table, format=args.format)
        print(data)
        return 0

    elif args.command == "cleanup":
        dry_run = not args.execute
        result = agent.cleanup_orphan_records(args.table, dry_run=dry_run)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["status"] in ("ok", "preview") else 1

    elif args.command == "count":
        if args.doc_id:
            counts = agent.count_records_by_document(args.doc_id)
        elif args.muni_id:
            counts = agent.count_records_by_municipio(args.muni_id)
        else:
            print("Especifica --doc-id o --muni-id")
            return 1
        print(json.dumps(counts, indent=2))
        return 0

    else:
        print("Comando no reconocido. Usa --help para ver opciones.")
        return 1


def handle_pipeline_command(agent: PipelineAgent, args) -> int:
    """Maneja comandos del agente Pipeline."""
    if args.command == "health":
        result = agent.health_check()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["status"] == "ok" else 1

    elif args.command == "test":
        if args.parser:
            result = agent.run_parser_on_pdf(args.pdf_path, args.parser)
            print(f"Parser: {result.parser_name}")
            print(f"Filas: {len(result.rows)}")
            print(f"Tiempo: {result.execution_time_ms:.0f}ms")
            print(f"Warnings: {len(result.warnings)}")
            print(f"Errors: {result.errors}")
            if result.rows:
                print("\nPrimeras 3 filas:")
                for row in result.rows[:3]:
                    print(f"  {row}")
        else:
            results = agent.run_all_parsers_on_pdf(args.pdf_path)
            report = agent.generate_parsing_report(results)
            print(report)
        return 0

    elif args.command == "extract-text":
        pdf_bytes = agent.load_pdf(args.pdf_path)

        page_range = None
        if args.pages:
            parts = args.pages.split("-")
            page_range = (int(parts[0]), int(parts[1]) if len(parts) > 1 else int(parts[0]))

        pages = agent.extract_text_by_page(pdf_bytes, page_range)

        output_text = ""
        for p in pages:
            output_text += f"\n=== PAGINA {p['page_num']} ({p['char_count']} chars) ===\n"
            output_text += p["text"]

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output_text)
            print(f"Texto guardado en: {args.output}")
        else:
            print(output_text)
        return 0

    elif args.command == "debug":
        result = agent.debug_extraction(args.pdf_path)
        print(f"PDF: {result['pdf_path']}")
        print(f"Paginas: {result['total_pages']}")
        print(f"Caracteres totales: {result['total_chars']}")
        print(f"Promedio chars/pagina: {result['avg_chars_per_page']}")
        if result['empty_pages']:
            print(f"Paginas vacias: {result['empty_pages']}")

        if args.section:
            text = "\n".join(p["text"] for p in result["pages"])
            section_result = agent.find_section(text, args.section)
            print(f"\nBusqueda de '{args.section}':")
            if section_result["found"]:
                print(f"  Encontrado en posicion {section_result['start_pos']}")
                print(f"  Linea aprox: {section_result['line_approx']}")
                print(f"  Preview: {section_result['preview'][:200]}...")
            else:
                print("  No encontrado")
                if section_result.get("suggestions"):
                    print("  Sugerencias:")
                    for s in section_result["suggestions"]:
                        print(f"    - {s}")
        return 0

    elif args.command == "generate-sample":
        if args.type == "all":
            data = {
                "recursos": agent.generate_sample_recursos(args.count),
                "gastos": agent.generate_sample_gastos(args.count),
            }
        else:
            data = agent.generate_sample_data(args.type, args.count)

        output = json.dumps(data, indent=2, ensure_ascii=False)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output)
            print(f"Datos guardados en: {args.output}")
        else:
            print(output)
        return 0

    elif args.command == "benchmark":
        pdf_bytes = agent.load_pdf(args.pdf_path)
        text = agent.extract_text(pdf_bytes)
        results = agent.benchmark_all_parsers(text, args.iterations)

        print("=== BENCHMARK RESULTS ===\n")
        for name, r in results.items():
            print(f"{name}:")
            print(f"  Avg: {r.avg_time_ms:.2f}ms")
            print(f"  Min: {r.min_time_ms:.2f}ms")
            print(f"  Max: {r.max_time_ms:.2f}ms")
        return 0

    else:
        print("Comando no reconocido. Usa --help para ver opciones.")
        return 1


def handle_streamlit_command(agent: StreamlitAgent, args) -> int:
    """Maneja comandos del agente Streamlit."""
    if args.command == "health":
        result = agent.health_check()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    elif args.command == "generate":
        name = args.name or args.table.replace("bd_", "").title()

        if args.component == "tab":
            code = agent.generate_tab_boilerplate(name, args.table)
        elif args.component == "editor":
            code = agent.generate_data_editor_boilerplate(args.table)
        elif args.component == "delete-ui":
            code = agent.generate_delete_ui_boilerplate(args.table)
        elif args.component == "crud":
            code = agent.generate_full_crud_tab(args.table, name)
        elif args.component == "form":
            # Generar form básico con campos de la tabla
            config = agent.get_table_ui_config(args.table)
            fields = [
                {"name": col, "type": "text", "label": col.replace("_", " ")}
                for col in (config.get("editable", [])[:5] if config else [])
            ]
            code = agent.generate_form_boilerplate(name, fields)
        else:
            print(f"Componente '{args.component}' no soportado")
            return 1

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(code)
            print(f"Codigo guardado en: {args.output}")
        else:
            print(code)
        return 0

    elif args.command == "mock":
        if args.type == "municipio":
            data = [agent.generate_mock_municipio() for _ in range(args.count)]
        elif args.type == "document":
            data = [agent.generate_mock_document() for _ in range(args.count)]
        elif args.type == "session-state":
            data = agent.generate_mock_session_state()
        else:
            data = agent.generate_mock_table_data(args.type, args.count)

        output = json.dumps(data, indent=2, ensure_ascii=False)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output)
            print(f"Datos guardados en: {args.output}")
        else:
            print(output)
        return 0

    elif args.command == "validate-session":
        if args.state_file:
            with open(args.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
        else:
            state = {}

        issues = agent.validate_session_state(state)
        if issues:
            print("Issues encontrados:")
            for issue in issues:
                print(f"  [{issue.issue_type}] {issue.key}: {issue.recommendation}")
        else:
            print("Session state valido")
        return 0

    elif args.command == "list-tables":
        tables = agent.list_available_tables()
        print("Tablas con configuracion UI:")
        for t in tables:
            config = agent.get_table_ui_config(t)
            print(f"  {t}")
            print(f"    PK: {config.get('pk')}")
            print(f"    Editables: {len(config.get('editable', []))} columnas")
        return 0

    else:
        print("Comando no reconocido. Usa --help para ver opciones.")
        return 1


def main(args: List[str] = None) -> int:
    """Punto de entrada principal del CLI."""
    parser = create_parser()
    parsed = parser.parse_args(args)

    if not parsed.agent:
        parser.print_help()
        return 0

    verbose = parsed.verbose

    try:
        if parsed.agent == "supabase":
            config = AgentConfig.from_streamlit_secrets()
            agent = SupabaseAgent(config=config, verbose=verbose)
            return handle_supabase_command(agent, parsed)

        elif parsed.agent == "pipeline":
            agent = PipelineAgent(verbose=verbose)
            return handle_pipeline_command(agent, parsed)

        elif parsed.agent == "streamlit":
            agent = StreamlitAgent(verbose=verbose)
            return handle_streamlit_command(agent, parsed)

        else:
            parser.print_help()
            return 1

    except Exception as e:
        print(f"Error: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
