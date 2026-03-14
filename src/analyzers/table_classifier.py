"""Classify tables as fact, dimension, lookup, audit, junction, or unknown."""
from __future__ import annotations

from typing import List

FACT_HINTS = {
    "fact_", "_fact", "transaction", "sale", "order", "event",
    "activity", "metric", "measure", "record", "entry", "purchase",
    "payment", "invoice", "shipment", "booking",
}
DIM_HINTS = {
    "dim_", "_dim", "dimension", "customer", "product", "employee",
    "user", "account", "store", "location", "category", "supplier",
    "vendor", "region", "territory", "department", "brand",
}
LOOKUP_HINTS = {
    "lookup_", "_lookup", "_code", "_type", "_status", "_ref",
    "reference", "config", "setting", "parameter", "constant",
    "currency", "country", "language", "timezone",
}
AUDIT_HINTS = {
    "_audit", "_log", "_history", "_archive", "_trail",
    "_changelog", "audit_", "log_", "_change", "_event_log",
}
JUNCTION_HINTS = {
    "_map", "_xref", "_link", "_assoc", "_rel",
    "_bridge", "_junction", "_pivot", "_through", "_m2m",
}

TABLE_TYPE_COLORS = {
    "fact":      "#7c3aed",   # violet
    "dimension": "#2563eb",   # blue
    "lookup":    "#059669",   # green
    "audit":     "#d97706",   # amber
    "junction":  "#0891b2",   # cyan
    "unknown":   "#4f46e5",   # indigo
}

TABLE_TYPE_LABELS = {
    "fact":      "Fact",
    "dimension": "Dimension",
    "lookup":    "Lookup",
    "audit":     "Audit",
    "junction":  "Junction",
    "unknown":   "",
}


def classify(table_name: str, columns: list) -> str:
    name = table_name.lower()
    fk_count = sum(1 for c in columns if c.is_foreign_key)
    col_count = len(columns)

    # Junction: ≤5 cols, ≥2 FKs — or named like one
    is_junction_name = any(h in name for h in JUNCTION_HINTS)
    if fk_count >= 2 and col_count <= 5:
        return "junction"
    if is_junction_name:
        return "junction"

    for hint in AUDIT_HINTS:
        if hint in name:
            return "audit"

    for hint in LOOKUP_HINTS:
        if name.startswith(hint.rstrip("_")) or hint in name:
            return "lookup"

    for hint in DIM_HINTS:
        if name.startswith("dim_") or name.endswith("_dim") or hint in name:
            return "dimension"

    for hint in FACT_HINTS:
        if name.startswith("fact_") or name.endswith("_fact") or hint in name:
            return "fact"

    # Heuristic: many FKs → likely fact
    if fk_count >= 3:
        return "fact"
    # Few small cols, no FKs → likely lookup
    if col_count <= 4 and fk_count == 0:
        return "lookup"

    return "unknown"


def classify_tables(tables_info: list) -> None:
    """Set table_type on every TableInfo in-place."""
    for t in tables_info:
        t.table_type = classify(t.name, t.columns)
