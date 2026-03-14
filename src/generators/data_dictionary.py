from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader
from ..db.inspector import TableInfo
from ..analyzers.table_classifier import TABLE_TYPE_COLORS, TABLE_TYPE_LABELS


def _col(c) -> Dict[str, Any]:
    return {
        "name": c.name, "type": c.type, "nullable": c.nullable,
        "default": c.default, "is_primary_key": c.is_primary_key,
        "is_foreign_key": c.is_foreign_key, "foreign_key_to": c.foreign_key_to,
        "comment": c.comment, "llm_description": c.llm_description,
        "llm_confidence": c.llm_confidence,
        "pii_type": c.pii_type, "pii_detected": c.pii_detected,
        "lineage_tags": c.lineage_tags, "naming_issue": c.naming_issue,
    }


def _table(t: TableInfo) -> Dict[str, Any]:
    return {
        "name": t.name, "schema": t.schema, "row_count": t.row_count,
        "comment": t.comment, "llm_description": t.llm_description,
        "llm_confidence": t.llm_confidence,
        "table_type": t.table_type,
        "primary_keys": t.primary_keys,
        "foreign_keys": [
            {"name": fk.name, "constrained_columns": fk.constrained_columns,
             "referred_table": fk.referred_table, "referred_columns": fk.referred_columns,
             "label": fk.label()}
            for fk in t.foreign_keys
        ],
        "indexes": [
            {"name": idx.get("name"), "columns": idx.get("column_names", []),
             "unique": idx.get("unique", False)}
            for idx in t.indexes
        ],
        "fk_integrity": t.fk_integrity,
        "sample_data": t.sample_data,
        "columns": [_col(c) for c in t.columns],
    }



def build_data(tables, db_name, generated_at):
    from ..analyzers.table_classifier import TABLE_TYPE_COLORS, TABLE_TYPE_LABELS
    return {
        "db_name": db_name, "generated_at": generated_at,
        "total_tables": len(tables),
        "total_columns": sum(len(t.columns) for t in tables),
        "total_rows": sum(t.row_count for t in tables if t.row_count >= 0),
        "tables": [_table(t) for t in tables],
        "type_colors": TABLE_TYPE_COLORS,
        "type_labels": TABLE_TYPE_LABELS,
    }

def generate(tables: List[TableInfo], db_name: str, output_dir: Path, generated_at: str) -> Path:
    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent.parent / "templates")), autoescape=False)
    data = {
        "db_name": db_name, "generated_at": generated_at,
        "total_tables": len(tables),
        "total_columns": sum(len(t.columns) for t in tables),
        "total_rows": sum(t.row_count for t in tables if t.row_count >= 0),
        "tables": [_table(t) for t in tables],
        "type_colors": TABLE_TYPE_COLORS,
        "type_labels": TABLE_TYPE_LABELS,
    }
    html = env.get_template("data_dictionary.html").render(data=data, data_json=json.dumps(data, default=str))
    out = output_dir / "data_dictionary.html"
    out.write_text(html, encoding="utf-8")
    return out
