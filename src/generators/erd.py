from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader
from ..db.inspector import TableInfo
from ..analyzers.table_classifier import TABLE_TYPE_COLORS, TABLE_TYPE_LABELS


def _build_nodes_edges(tables):
    nodes = []
    for t in tables:
        nodes.append({
            "id": t.name, "label": t.name, "name": t.name, "schema": t.schema,
            "row_count": t.row_count, "column_count": len(t.columns),
            "table_type": t.table_type,
            "type_color": TABLE_TYPE_COLORS.get(t.table_type, "#4f46e5"),
            "type_label": TABLE_TYPE_LABELS.get(t.table_type, ""),
            "columns": [
                {"name": c.name, "type": c.type,
                 "is_primary_key": c.is_primary_key, "is_foreign_key": c.is_foreign_key,
                 "foreign_key_to": c.foreign_key_to, "nullable": c.nullable,
                 "pii_type": c.pii_type}
                for c in t.columns
            ],
        })

    edges, seen = [], set()
    for t in tables:
        for fk in t.foreign_keys:
            src_col = ", ".join(fk.constrained_columns)
            tgt_col = ", ".join(fk.referred_columns)
            edge_id = f"{t.name}_{fk.referred_table}_{src_col}"
            if edge_id in seen:
                continue
            seen.add(edge_id)
            edges.append({
                "id": edge_id, "source": t.name, "target": fk.referred_table,
                "source_col": src_col, "target_col": tgt_col,
                "label": f"{src_col} → {tgt_col}",
                "cardinality": "N:1",
                "inferred": fk.inferred,   # True = heuristic, renders as dashed
            })
    return nodes, edges


def build_data(tables, db_name, generated_at):
    nodes, edges = _build_nodes_edges(tables)
    return {
        "db_name": db_name, "generated_at": generated_at,
        "nodes": nodes, "edges": edges,
        "type_colors": TABLE_TYPE_COLORS,
        "type_labels": TABLE_TYPE_LABELS,
    }


def generate(tables: List[TableInfo], db_name: str, output_dir: Path, generated_at: str) -> Path:
    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent.parent / "templates")), autoescape=False)
    nodes, edges = _build_nodes_edges(tables)
    data = {
        "db_name": db_name, "generated_at": generated_at,
        "nodes": nodes, "edges": edges,
        "type_colors": TABLE_TYPE_COLORS,
        "type_labels": TABLE_TYPE_LABELS,
    }
    html = env.get_template("erd.html").render(data=data, data_json=json.dumps(data, default=str))
    out = output_dir / "erd.html"
    out.write_text(html, encoding="utf-8")
    return out
