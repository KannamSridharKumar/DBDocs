from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


@dataclass
class ForeignKeyInfo:
    name: Optional[str]
    constrained_columns: List[str]
    referred_schema: Optional[str]
    referred_table: str
    referred_columns: List[str]
    inferred: bool = False          # True = heuristic guess, not a real FK constraint

    def label(self) -> str:
        src = ", ".join(self.constrained_columns)
        dst = ", ".join(self.referred_columns)
        return f"{src} → {self.referred_table}.{dst}"


@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    default: Optional[str]
    is_primary_key: bool
    is_foreign_key: bool
    foreign_key_to: Optional[str]
    comment: Optional[str] = None
    llm_description: Optional[str] = None
    llm_confidence: Optional[int] = None   # 0-100
    # enriched by analyzers
    pii_type: Optional[str] = None
    lineage_tags: List[str] = field(default_factory=list)
    naming_issue: Optional[str] = None
    # value-level PII from profiler
    pii_detected: Optional[str] = None


@dataclass
class TableInfo:
    name: str
    schema: Optional[str]
    columns: List[ColumnInfo]
    row_count: int
    comment: Optional[str]
    primary_keys: List[str]
    foreign_keys: List[ForeignKeyInfo]
    indexes: List[Dict[str, Any]]
    sample_data: List[Dict[str, Any]]
    llm_description: Optional[str] = None
    llm_confidence: Optional[int] = None   # 0-100
    # enriched by analyzers
    table_type: str = "unknown"
    fk_integrity: List[Dict] = field(default_factory=list)


class SchemaInspector:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._insp = inspect(engine)

    def get_table_names(self, schema: Optional[str] = None) -> List[str]:
        try:
            return sorted(self._insp.get_table_names(schema=schema))
        except Exception as exc:
            raise RuntimeError(f"Failed to list tables: {exc}") from exc

    def filter_tables(self, tables: List[str], table_filter: Optional[str]) -> List[str]:
        if not table_filter:
            return tables
        patterns = [p.strip() for p in table_filter.split(",") if p.strip()]
        result = []
        for table in tables:
            for pat in patterns:
                if re.fullmatch(pat, table, re.IGNORECASE):
                    result.append(table)
                    break
        return result

    def get_table_info(self, table_name: str, schema: Optional[str] = None, sample_rows: int = 5) -> TableInfo:
        columns_raw = self._insp.get_columns(table_name, schema=schema)
        pk_info = self._insp.get_pk_constraint(table_name, schema=schema)
        primary_keys: List[str] = pk_info.get("constrained_columns", [])

        fks_raw = self._insp.get_foreign_keys(table_name, schema=schema)
        foreign_keys = [
            ForeignKeyInfo(
                name=fk.get("name"),
                constrained_columns=fk["constrained_columns"],
                referred_schema=fk.get("referred_schema"),
                referred_table=fk["referred_table"],
                referred_columns=fk["referred_columns"],
            )
            for fk in fks_raw
        ]

        try:
            indexes_raw = self._insp.get_indexes(table_name, schema=schema)
            unique_cols: set = {
                c for idx in indexes_raw if idx.get("unique") for c in idx.get("column_names", [])
            }
        except Exception:
            indexes_raw = []
            unique_cols = set()

        fk_map: Dict[str, str] = {}
        for fk in fks_raw:
            for local, remote in zip(fk["constrained_columns"], fk["referred_columns"]):
                fk_map[local] = f"{fk['referred_table']}.{remote}"
            # Attach cardinality hint
            lc = fk["constrained_columns"][0] if fk["constrained_columns"] else None
            fk["cardinality"] = "1:1" if (lc and (lc in primary_keys or lc in unique_cols)) else "N:1"

        columns = []
        for col in columns_raw:
            col_name = col["name"]
            default = col.get("default")
            columns.append(ColumnInfo(
                name=col_name,
                type=str(col.get("type", "UNKNOWN")).upper(),
                nullable=bool(col.get("nullable", True)),
                default=str(default) if default is not None else None,
                is_primary_key=col_name in primary_keys,
                is_foreign_key=col_name in fk_map,
                foreign_key_to=fk_map.get(col_name),
                comment=col.get("comment"),
            ))

        return TableInfo(
            name=table_name,
            schema=schema,
            columns=columns,
            row_count=self._safe_row_count(table_name, schema),
            comment=self._safe_table_comment(table_name, schema),
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=indexes_raw,
            sample_data=self._safe_sample_data(table_name, schema, sample_rows),
        )

    def _safe_row_count(self, table_name, schema):
        q = self._q(table_name, schema)
        try:
            with self.engine.connect() as conn:
                return int(conn.execute(text(f"SELECT COUNT(*) FROM {q}")).scalar() or 0)
        except Exception:
            return -1

    def _safe_sample_data(self, table_name, schema, n):
        q = self._q(table_name, schema)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {q} LIMIT {n}"))
                cols = list(result.keys())
                return [{k: self._json_safe(v) for k, v in zip(cols, row)} for row in result.fetchall()]
        except Exception:
            return []

    def _safe_table_comment(self, table_name, schema):
        try:
            return self._insp.get_table_comment(table_name, schema=schema).get("text") or None
        except Exception:
            return None

    @staticmethod
    def _q(table_name, schema):
        return f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

    @staticmethod
    def _json_safe(v):
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat()
        try:
            json.dumps(v)
            return v
        except (TypeError, ValueError):
            return str(v)

    def get_all_tables(self, schema=None, table_filter=None, sample_rows=5, progress_callback=None):
        all_names = self.get_table_names(schema)
        names = self.filter_tables(all_names, table_filter)
        results = []
        for i, name in enumerate(names):
            if progress_callback:
                progress_callback(i, len(names), name)
            try:
                results.append(self.get_table_info(name, schema, sample_rows))
            except Exception as exc:
                print(f"  [warn] Skipping '{name}': {exc}")
        return results
