#!/usr/bin/env python3
"""
db-data-dict: Generate interactive HTML documentation for any SQL database.

Usage:
    python main.py [OPTIONS]

All options can also be set in .env or db-data-dict.yaml (see .env.example).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

app = typer.Typer(add_completion=False, rich_markup_mode="rich")
console = Console()


def _load_yaml_config(path: Path) -> dict:
    """Load optional YAML config file (db-data-dict.yaml in CWD)."""
    try:
        import yaml  # type: ignore
        if path.exists():
            with path.open() as f:
                return yaml.safe_load(f) or {}
    except ImportError:
        pass  # PyYAML not installed – silently skip
    except Exception as exc:
        console.print(f"  [yellow]⚠ YAML config warning:[/yellow] {exc}")
    return {}


@app.command()
def generate(
    db_url: Optional[str] = typer.Option(
        None, "--db-url", help="Database URL (overrides DB_URL in .env)"
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        "-t",
        help="Comma-separated table names or regex patterns (overrides TABLE_FILTER)",
    ),
    schema: Optional[str] = typer.Option(
        None, "--schema", help="Database schema (overrides DB_SCHEMA)"
    ),
    output_dir: Optional[Path] = typer.Option(
        None, "--output-dir", "-o",
        help="Output directory for HTML files (overrides OUTPUT_DIR in .env)"
    ),
    skip_llm: bool = typer.Option(False, "--skip-llm", help="Skip LLM description generation"),
    skip_profiling: bool = typer.Option(
        False, "--skip-profiling", help="Skip data profiling (faster)"
    ),
    skip_erd: bool = typer.Option(False, "--skip-erd", help="Skip ERD generation"),
    skip_integrity: bool = typer.Option(
        False, "--skip-integrity", help="Skip FK referential integrity check"
    ),
    no_cache: bool = typer.Option(
        False, "--no-cache", help="Force regenerate LLM descriptions (ignore cache)"
    ),
    sample_rows: int = typer.Option(
        100_000, "--sample-rows", help="Max rows to sample per table for profiling"
    ),
    export: Optional[str] = typer.Option(
        None,
        "--export",
        help="Comma-separated export formats: html, json, csv, all (overrides EXPORT in .env)",
    ),
    config_file: Optional[Path] = typer.Option(
        None, "--config", "-c", help="YAML config file (default: ./db-data-dict.yaml)"
    ),
) -> None:
    """Generate interactive HTML data dictionary, profiling report, and ERD."""

    console.print(
        Panel.fit(
            "[bold white]db-data-dict[/bold white] · [dim]Database Documentation Generator[/dim]",
            border_style="bright_magenta",
        )
    )

    # ── Load YAML config (lowest priority) ────────────────────────────────
    yaml_path = config_file or Path("db-data-dict.yaml")
    yaml_cfg = _load_yaml_config(yaml_path)
    if yaml_cfg and yaml_path.exists():
        console.print(f"  [dim]Config file: {yaml_path}[/dim]")

    # ── Load .env config ───────────────────────────────────────────────────
    from src.config import load_config
    cfg = load_config()

    # Apply YAML overrides (yaml > .env)
    if yaml_cfg.get("db_url"):
        cfg.db_url = yaml_cfg["db_url"]
    if yaml_cfg.get("db_schema"):
        cfg.db_schema = yaml_cfg["db_schema"]
    if yaml_cfg.get("table_filter"):
        cfg.table_filter = yaml_cfg["table_filter"]
    if yaml_cfg.get("llm_provider"):
        cfg.llm_provider = yaml_cfg["llm_provider"]
    if yaml_cfg.get("llm_model"):
        cfg.llm_model = yaml_cfg["llm_model"]
    if yaml_cfg.get("max_sample_rows"):
        cfg.max_sample_rows = int(yaml_cfg["max_sample_rows"])

    # CLI overrides everything (only when explicitly provided)
    if db_url:
        cfg.db_url = db_url
    if tables:
        cfg.table_filter = tables
    if schema:
        cfg.db_schema = schema
    if sample_rows != 100_000:
        cfg.max_sample_rows = sample_rows

    # Resolve output_dir: CLI flag > OUTPUT_DIR in .env > built-in default
    if output_dir is None:
        output_dir = Path(cfg.output_dir)

    # Resolve export: CLI flag > EXPORT in .env > built-in default
    if export is None:
        export = cfg.export_formats

    # Parse export formats
    export_formats = set(f.strip().lower() for f in export.split(","))
    if "all" in export_formats:
        export_formats = {"html", "json", "csv"}
    do_html = "html" in export_formats
    do_json = "json" in export_formats
    do_csv = "csv" in export_formats

    if not cfg.db_url:
        console.print("[red]Error:[/red] No database URL provided. Set DB_URL in .env or use --db-url.")
        raise typer.Exit(1)

    # ── Connect to database ────────────────────────────────────────────────
    console.print(f"\n[bold]Connecting to database…[/bold]")
    from src.db.connector import create_db_engine, get_db_name

    try:
        engine = create_db_engine(cfg.db_url)
        db_name = get_db_name(cfg.db_url)
        console.print(f"  [green]✓[/green] Connected to [bold]{db_name}[/bold]")
    except Exception as exc:
        console.print(f"  [red]✗ Connection failed:[/red] {exc}")
        raise typer.Exit(1)

    # ── Inspect schema ─────────────────────────────────────────────────────
    console.print(f"\n[bold]Inspecting schema…[/bold]")
    from src.db.inspector import SchemaInspector

    inspector = SchemaInspector(engine)
    all_table_names = inspector.get_table_names(cfg.db_schema)
    filtered_names = inspector.filter_tables(all_table_names, cfg.table_filter)

    if not filtered_names:
        console.print(
            f"  [yellow]Warning:[/yellow] No tables found "
            f"(filter='{cfg.table_filter}', schema='{cfg.db_schema}')"
        )
        raise typer.Exit(1)

    console.print(
        f"  [green]✓[/green] Found [bold]{len(filtered_names)}[/bold] tables "
        f"(of {len(all_table_names)} total)"
    )
    if cfg.table_filter:
        console.print(f"  [dim]Filter: {cfg.table_filter}[/dim]")

    # Inspect tables with progress
    console.print(f"\n[bold]Loading table metadata…[/bold]")
    tables_info = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Reading tables", total=len(filtered_names))
        for name in filtered_names:
            progress.update(task, description=f"  {name[:40]}")
            try:
                info = inspector.get_table_info(name, cfg.db_schema, cfg.sample_data_rows)
                tables_info.append(info)
            except Exception as exc:
                console.print(f"  [yellow]⚠[/yellow] Skipping '{name}': {exc}")
            progress.advance(task)

    console.print(f"  [green]✓[/green] Loaded {len(tables_info)} tables")

    # ── Analyzers ──────────────────────────────────────────────────────────
    console.print(f"\n[bold]Running analyzers…[/bold]")
    from src.analyzers.table_classifier import classify_tables
    from src.analyzers.pii_detector import tag_tables as tag_pii
    from src.analyzers.lineage_tagger import tag_tables as tag_lineage
    from src.analyzers.naming_convention import analyze_tables

    classify_tables(tables_info)
    console.print("  [green]✓[/green] Table classification")

    from src.analyzers.fk_inferrer import infer_implicit_fks
    infer_implicit_fks(tables_info)
    inferred_count = sum(
        sum(1 for fk in t.foreign_keys if fk.inferred) for t in tables_info
    )
    if inferred_count:
        console.print(f"  [green]✓[/green] Inferred {inferred_count} implicit FK relationship(s) from column names")
    tag_pii(tables_info)
    console.print("  [green]✓[/green] PII detection (column names)")
    tag_lineage(tables_info)
    console.print("  [green]✓[/green] Lineage tagging")
    analyze_tables(tables_info)
    console.print("  [green]✓[/green] Naming convention analysis")

    # ── FK Referential Integrity ───────────────────────────────────────────
    if not skip_integrity:
        console.print(f"\n[bold]Checking FK referential integrity…[/bold]")
        from src.analyzers.referential_integrity import check_all
        try:
            check_all(engine, tables_info)
            total_viols = sum(
                1 for t in tables_info
                for fk in t.fk_integrity
                if fk.get("status") == "violation"
            )
            if total_viols:
                console.print(f"  [yellow]⚠[/yellow] {total_viols} FK violation(s) found")
            else:
                console.print("  [green]✓[/green] No FK violations")
        except Exception as exc:
            console.print(f"  [yellow]⚠ Integrity check failed:[/yellow] {exc}")

    # ── Data profiling ─────────────────────────────────────────────────────
    profiles = []
    profile_map: dict = {}  # populated after profiling; used by LLM section
    if not skip_profiling:
        console.print(f"\n[bold]Profiling data…[/bold]")
        from src.db.profiler import DataProfiler

        profiler = DataProfiler(engine, cfg.max_sample_rows, cfg.top_values_count)
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Profiling", total=len(tables_info))
            for t in tables_info:
                progress.update(task, description=f"  {t.name[:40]}")
                p = profiler.profile_table(t.name, cfg.db_schema)
                if p:
                    # Copy FK integrity from tables_info to profile
                    p.fk_integrity = t.fk_integrity
                    profiles.append(p)
                progress.advance(task)
        console.print(f"  [green]✓[/green] Profiled {len(profiles)} tables")

        # Merge row counts back
        profile_map = {p.name: p for p in profiles}
        for t in tables_info:
            if t.name in profile_map:
                t.row_count = profile_map[t.name].row_count or t.row_count

    # ── LLM descriptions ───────────────────────────────────────────────────
    if not skip_llm and cfg.llm_provider != "none":
        console.print(f"\n[bold]Generating LLM descriptions ({cfg.llm_provider}/{cfg.llm_model})…[/bold]")
        from src.llm.client import LLMClient

        output_dir.mkdir(parents=True, exist_ok=True)
        data_dir = output_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        cache_path = data_dir / ".llm_cache.json"
        llm_kwargs: dict = {}
        if cfg.llm_provider == "openai":
            llm_kwargs["api_key"] = cfg.openai_api_key
        elif cfg.llm_provider == "anthropic":
            llm_kwargs["api_key"] = cfg.anthropic_api_key
        elif cfg.llm_provider == "ollama":
            llm_kwargs["base_url"] = cfg.ollama_base_url
        elif cfg.llm_provider == "openai_compatible":
            llm_kwargs["api_key"] = cfg.openai_compatible_api_key
            llm_kwargs["base_url"] = cfg.openai_compatible_base_url
        elif cfg.llm_provider == "openrouter":
            llm_kwargs["api_key"] = cfg.openrouter_api_key

        try:
            llm = LLMClient(
                cfg.llm_provider,
                cfg.llm_model,
                cache_path=cache_path,
                no_cache=no_cache,
                call_delay=cfg.llm_call_delay,
                log_path=data_dir / "llm_calls.log",
                **llm_kwargs,
            )
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console,
            ) as progress:
                task = progress.add_task("Describing tables", total=len(tables_info))
                for t in tables_info:
                    progress.update(task, description=f"  {t.name[:40]}")
                    # Get per-column profiles if profiling was run
                    t_profile = profile_map.get(t.name)
                    col_prof_map = (
                        {cp.name: cp for cp in t_profile.columns}
                        if t_profile else None
                    )
                    t.llm_description, t.llm_confidence = llm.describe_table(
                        t.name, t.columns, t.row_count,
                        table_type=t.table_type,
                        table_profile=t_profile,
                    )
                    col_descs = llm.describe_columns(
                        t.name, t.columns,
                        col_profiles=col_prof_map,
                    )
                    for col in t.columns:
                        if col.name in col_descs:
                            col.llm_description, col.llm_confidence = col_descs[col.name]
                    progress.advance(task)
            console.print(f"  [green]✓[/green] LLM descriptions generated (cached at {cache_path})")
        except Exception as exc:
            console.print(f"  [yellow]⚠ LLM failed:[/yellow] {exc}")
    elif not skip_llm and cfg.llm_provider == "none":
        console.print(
            "\n[dim]LLM disabled (LLM_PROVIDER=none). Use --skip-llm to suppress this message "
            "or set LLM_PROVIDER in .env.[/dim]"
        )

    # ── Generate outputs ───────────────────────────────────────────────────
    console.print(f"\n[bold]Generating outputs ({', '.join(sorted(export_formats))})…[/bold]")
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    from src.generators import data_dictionary, data_profiling, erd, index_page

    # Data dictionary
    if do_html:
        dd_path = data_dictionary.generate(tables_info, db_name, output_dir, generated_at)
        console.print(f"  [green]✓[/green] Data Dictionary  → {dd_path}")

    # Data profiling
    dp_path = None
    if do_html and not skip_profiling and profiles:
        dp_path = data_profiling.generate(profiles, db_name, output_dir, generated_at)
        console.print(f"  [green]✓[/green] Data Profiling   → {dp_path}")

    # ERD
    erd_path = None
    if do_html and not skip_erd:
        erd_path = erd.generate(tables_info, db_name, output_dir, generated_at)
        console.print(f"  [green]✓[/green] ERD              → {erd_path}")

    # Index page
    profile_map = {p.name: p for p in profiles}
    table_summaries = []
    for t in tables_info:
        p = profile_map.get(t.name)
        table_summaries.append(
            {
                "name": t.name,
                "row_count": t.row_count,
                "column_count": len(t.columns),
                "quality_score": p.quality_score if p else 100.0,
                "table_type": t.table_type,
            }
        )

    if do_html:
        idx_path = index_page.generate(
            db_name=db_name,
            generated_at=generated_at,
            output_dir=output_dir,
            table_summaries=table_summaries,
            llm_provider=cfg.llm_provider,
            llm_model=cfg.llm_model,
            has_profiling=dp_path is not None,
            has_erd=erd_path is not None,
        )
        console.print(f"  [green]✓[/green] Index            → {idx_path}")

    # JSON exports
    if do_json:
        from src.exporters.json_exporter import export as json_export
        from src.generators.data_dictionary import build_data as dd_build
        from src.generators.data_profiling import build_data as dp_build
        from src.generators.erd import build_data as erd_build
        try:
            dd_data = dd_build(tables_info, db_name, generated_at)
            dp_data = dp_build(profiles, db_name, generated_at) if profiles else {"tables": []}
            erd_data = erd_build(tables_info, db_name, generated_at)
            json_export(data_dir, dd_data, dp_data, erd_data)
            console.print(f"  [green]✓[/green] JSON exports     → {data_dir}/")
        except Exception as exc:
            console.print(f"  [yellow]⚠ JSON export failed:[/yellow] {exc}")

    # CSV exports
    if do_csv:
        from src.exporters.csv_exporter import export as csv_export
        try:
            if "dd_data" not in dir():
                from src.generators.data_dictionary import build_data as dd_build2
                from src.generators.data_profiling import build_data as dp_build2
                dd_data = dd_build2(tables_info, db_name, generated_at)
                dp_data = dp_build2(profiles, db_name, generated_at) if profiles else {"tables": []}
            csv_export(data_dir, dd_data, dp_data)
            console.print(f"  [green]✓[/green] CSV exports      → {data_dir}/")
        except Exception as exc:
            console.print(f"  [yellow]⚠ CSV export failed:[/yellow] {exc}")

    # ── Summary ────────────────────────────────────────────────────────────
    summary = Table(show_header=False, box=None, padding=(0, 1))
    summary.add_column(style="dim")
    summary.add_column()
    summary.add_row("Database", db_name)
    summary.add_row("Tables", str(len(tables_info)))
    summary.add_row("Columns", str(sum(len(t.columns) for t in tables_info)))
    summary.add_row("Rows (total)", f"{sum(t.row_count for t in tables_info if t.row_count >= 0):,}")
    summary.add_row("LLM", cfg.llm_provider)
    summary.add_row("Exports", ", ".join(sorted(export_formats)))
    summary.add_row("Output dir", str(output_dir.resolve()))

    console.print()
    console.print(Panel(summary, title="[bold green]✓ Done[/bold green]", border_style="green"))
    if do_html:
        idx_path_final = output_dir / "index.html"
        console.print(f"\n  [bold]Open in browser:[/bold] {idx_path_final.resolve()}\n")


if __name__ == "__main__":
    app()
