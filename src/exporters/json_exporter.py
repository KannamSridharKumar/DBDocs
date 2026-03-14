"""Export all data as standalone JSON files."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def _dump(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def export(
    data_dir: Path,
    dd_data: Dict,
    profiling_data: Dict,
    erd_data: Dict,
) -> None:
    """
    Write four JSON files to data_dir/:
      schema.json    – tables, columns, PKs, FKs, LLM descriptions
      profiling.json – per-column stats, quality scores
      erd.json       – graph nodes + edges
      summary.json   – top-level KPIs
    """
    data_dir.mkdir(parents=True, exist_ok=True)

    _dump(data_dir / "schema.json", dd_data)
    _dump(data_dir / "profiling.json", profiling_data)
    _dump(data_dir / "erd.json", erd_data)

    # Summary: lightweight rollup
    tables = dd_data.get("tables", [])
    prof_tables = profiling_data.get("tables", [])
    avg_quality = (
        sum(t.get("quality_score", 100) for t in prof_tables) / len(prof_tables)
        if prof_tables
        else None
    )
    pii_cols = [
        {"table": t["name"], "column": c["name"], "pii_type": c["pii_type"]}
        for t in tables
        for c in t.get("columns", [])
        if c.get("pii_type")
    ]
    fk_violations = [
        {
            "table": t["name"],
            "local_col": fk["local_col"],
            "ref_table": fk["ref_table"],
            "orphan_count": fk["orphan_count"],
        }
        for t in tables
        for fk in t.get("fk_integrity", [])
        if fk.get("status") == "violation"
    ]
    quality_issues = [
        {"table": t["name"], "issue": iss}
        for t in prof_tables
        for iss in t.get("issues", [])
    ]

    summary = {
        "db_name": dd_data.get("db_name"),
        "generated_at": dd_data.get("generated_at"),
        "total_tables": dd_data.get("total_tables"),
        "total_columns": dd_data.get("total_columns"),
        "total_rows": dd_data.get("total_rows"),
        "avg_quality_score": round(avg_quality, 1) if avg_quality is not None else None,
        "pii_columns_count": len(pii_cols),
        "pii_columns": pii_cols,
        "fk_violations_count": len(fk_violations),
        "fk_violations": fk_violations,
        "quality_issues_count": len(quality_issues),
        "quality_issues": quality_issues,
    }
    _dump(data_dir / "summary.json", summary)
