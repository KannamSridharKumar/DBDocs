from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader
from ..db.profiler import TableProfile


def _col(c) -> Dict[str, Any]:
    return {
        "name": c.name, "dtype": c.dtype,
        "null_count": c.null_count, "null_pct": c.null_pct,
        "distinct_count": c.distinct_count, "distinct_pct": c.distinct_pct,
        "min_val": c.min_val, "max_val": c.max_val, "mean_val": c.mean_val,
        "median_val": c.median_val, "std_val": c.std_val, "q25": c.q25, "q75": c.q75,
        "histogram": c.histogram,
        "outlier_count": c.outlier_count, "outlier_pct": c.outlier_pct,
        "min_length": c.min_length, "max_length": c.max_length, "avg_length": c.avg_length,
        "min_date": c.min_date, "max_date": c.max_date,
        "top_values": c.top_values,
        "detected_pattern": c.detected_pattern, "pii_detected": c.pii_detected,
        "is_constant": c.is_constant, "is_unique": c.is_unique,
    }


def _table(t: TableProfile) -> Dict[str, Any]:
    return {
        "name": t.name, "row_count": t.row_count, "sampled_rows": t.sampled_rows,
        "column_count": t.column_count, "total_null_cells": t.total_null_cells,
        "null_pct": t.null_pct, "duplicate_count": t.duplicate_count,
        "duplicate_pct": t.duplicate_pct, "quality_score": t.quality_score,
        "issues": t.issues, "trend_data": t.trend_data, "fk_integrity": t.fk_integrity,
        "columns": [_col(c) for c in t.columns],
    }



def build_data(profiles, db_name, generated_at):
    avg_quality = sum(p.quality_score for p in profiles) / len(profiles) if profiles else 100.0
    return {
        "db_name": db_name, "generated_at": generated_at,
        "total_tables": len(profiles),
        "total_rows": sum(p.row_count for p in profiles),
        "total_columns": sum(p.column_count for p in profiles),
        "avg_quality_score": round(avg_quality, 1),
        "tables": [_table(t) for t in profiles],
    }

def generate(profiles: List[TableProfile], db_name: str, output_dir: Path, generated_at: str) -> Path:
    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent.parent / "templates")), autoescape=False)
    avg_quality = sum(p.quality_score for p in profiles) / len(profiles) if profiles else 100.0
    data = {
        "db_name": db_name, "generated_at": generated_at,
        "total_tables": len(profiles),
        "total_rows": sum(p.row_count for p in profiles),
        "total_columns": sum(p.column_count for p in profiles),
        "avg_quality_score": round(avg_quality, 1),
        "tables": [_table(t) for t in profiles],
    }
    html = env.get_template("data_profiling.html").render(data=data, data_json=json.dumps(data, default=str))
    out = output_dir / "data_profiling.html"
    out.write_text(html, encoding="utf-8")
    return out
