from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader
from ..analyzers.table_classifier import TABLE_TYPE_COLORS, TABLE_TYPE_LABELS


def generate(
    db_name: str,
    generated_at: str,
    output_dir: Path,
    table_summaries: List[Dict[str, Any]],
    llm_provider: str,
    has_profiling: bool,
    has_erd: bool,
    llm_model: str = "",
) -> Path:
    template_dir = Path(__file__).parent.parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)
    template = env.get_template("index.html")

    total_rows = sum(t.get("row_count", 0) for t in table_summaries if t.get("row_count", 0) >= 0)
    total_cols = sum(t.get("column_count", 0) for t in table_summaries)
    avg_quality = (
        sum(t.get("quality_score", 100) for t in table_summaries) / len(table_summaries)
        if table_summaries
        else 100.0
    )

    data = {
        "db_name": db_name,
        "generated_at": generated_at,
        "llm_provider": llm_provider,
        "llm_model": llm_model,
        "has_profiling": has_profiling,
        "has_erd": has_erd,
        "total_tables": len(table_summaries),
        "total_rows": total_rows,
        "total_columns": total_cols,
        "avg_quality_score": round(avg_quality, 1),
        "tables": table_summaries,
        "type_colors": TABLE_TYPE_COLORS,
        "type_labels": TABLE_TYPE_LABELS,
    }

    html = template.render(data=data, data_json=json.dumps(data, default=str))
    out_path = output_dir / "index.html"
    out_path.write_text(html, encoding="utf-8")
    return out_path
