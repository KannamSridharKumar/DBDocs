"""Verify FK referential integrity by counting orphaned rows."""
from __future__ import annotations

from typing import List

from sqlalchemy import text
from sqlalchemy.engine import Engine


def check_all(engine: Engine, tables_info: list) -> None:
    """
    For every FK in every table, count rows in the child table whose FK
    value has no matching row in the referenced (parent) table.

    Results are written to TableInfo.fk_integrity as a list of dicts:
      {local_col, ref_table, ref_col, orphan_count, status, [error]}
    """
    for t in tables_info:
        t.fk_integrity = []
        for fk in t.foreign_keys:
            if not fk.constrained_columns or not fk.referred_columns:
                continue
            local_col = fk.constrained_columns[0]
            ref_table = fk.referred_table
            ref_col = fk.referred_columns[0]
            schema_pfx = f'"{t.schema}".' if t.schema else ""
            try:
                sql = text(
                    f'SELECT COUNT(*) FROM {schema_pfx}"{t.name}" c '
                    f'WHERE c."{local_col}" IS NOT NULL '
                    f'AND c."{local_col}" NOT IN '
                    f'(SELECT "{ref_col}" FROM "{ref_table}")'
                )
                with engine.connect() as conn:
                    orphans = int(conn.execute(sql).scalar() or 0)
                t.fk_integrity.append(
                    {
                        "fk_name": fk.name,
                        "local_col": local_col,
                        "ref_table": ref_table,
                        "ref_col": ref_col,
                        "orphan_count": orphans,
                        "status": "ok" if orphans == 0 else "violation",
                    }
                )
            except Exception as exc:
                t.fk_integrity.append(
                    {
                        "fk_name": fk.name,
                        "local_col": local_col,
                        "ref_table": ref_table,
                        "ref_col": ref_col,
                        "orphan_count": -1,
                        "status": "error",
                        "error": str(exc),
                    }
                )
