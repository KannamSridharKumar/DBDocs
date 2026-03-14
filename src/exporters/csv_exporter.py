"""Export flattened data as CSV files for use in BI tools / Excel."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional


def _w(path: Path, fieldnames: List[str], rows: List[Dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def export(
    csv_dir: Path,
    dd_data: Dict,
    profiling_data: Dict,
) -> None:
    """
    Write CSV files to csv_dir/:
      tables.csv        – one row per table
      columns.csv       – one row per column
      profiling.csv     – per-column stats
      relationships.csv – one row per FK relationship
      issues.csv        – data quality issues
      pii_report.csv    – detected PII columns
    """
    csv_dir.mkdir(parents=True, exist_ok=True)

    tables = dd_data.get("tables", [])
    prof_tables = profiling_data.get("tables", [])
    prof_map = {t["name"]: t for t in prof_tables}
    col_prof_map: Dict[str, Dict] = {}
    for pt in prof_tables:
        for c in pt.get("columns", []):
            col_prof_map[f"{pt['name']}.{c['name']}"] = c

    # ── tables.csv ─────────────────────────────────────────────────────
    _w(
        csv_dir / "tables.csv",
        ["table", "schema", "row_count", "column_count", "table_type",
         "quality_score", "null_pct", "duplicate_pct", "fk_violation_count",
         "issue_count", "llm_description"],
        [
            {
                "table": t["name"],
                "schema": t.get("schema") or "",
                "row_count": t.get("row_count", 0),
                "column_count": len(t.get("columns", [])),
                "table_type": t.get("table_type", "unknown"),
                "quality_score": prof_map.get(t["name"], {}).get("quality_score", ""),
                "null_pct": prof_map.get(t["name"], {}).get("null_pct", ""),
                "duplicate_pct": prof_map.get(t["name"], {}).get("duplicate_pct", ""),
                "fk_violation_count": sum(
                    1 for fk in t.get("fk_integrity", []) if fk.get("status") == "violation"
                ),
                "issue_count": len(prof_map.get(t["name"], {}).get("issues", [])),
                "llm_description": t.get("llm_description") or "",
            }
            for t in tables
        ],
    )

    # ── columns.csv ────────────────────────────────────────────────────
    col_rows = []
    for t in tables:
        for c in t.get("columns", []):
            col_rows.append(
                {
                    "table": t["name"],
                    "table_type": t.get("table_type", "unknown"),
                    "column": c["name"],
                    "type": c.get("type", ""),
                    "nullable": c.get("nullable", True),
                    "is_pk": c.get("is_primary_key", False),
                    "is_fk": c.get("is_foreign_key", False),
                    "fk_to": c.get("foreign_key_to") or "",
                    "default_value": c.get("default") or "",
                    "pii_type": c.get("pii_type") or "",
                    "lineage_tags": ",".join(c.get("lineage_tags", [])),
                    "naming_issue": c.get("naming_issue") or "",
                    "description": c.get("llm_description") or c.get("comment") or "",
                }
            )
    _w(
        csv_dir / "columns.csv",
        ["table", "table_type", "column", "type", "nullable", "is_pk", "is_fk",
         "fk_to", "default_value", "pii_type", "lineage_tags", "naming_issue", "description"],
        col_rows,
    )

    # ── profiling.csv ──────────────────────────────────────────────────
    prof_rows = []
    for pt in prof_tables:
        for c in pt.get("columns", []):
            prof_rows.append(
                {
                    "table": pt["name"],
                    "column": c["name"],
                    "dtype": c.get("dtype", ""),
                    "null_count": c.get("null_count", ""),
                    "null_pct": c.get("null_pct", ""),
                    "distinct_count": c.get("distinct_count", ""),
                    "distinct_pct": c.get("distinct_pct", ""),
                    "min_val": c.get("min_val", ""),
                    "max_val": c.get("max_val", ""),
                    "mean_val": c.get("mean_val", ""),
                    "median_val": c.get("median_val", ""),
                    "std_val": c.get("std_val", ""),
                    "q25": c.get("q25", ""),
                    "q75": c.get("q75", ""),
                    "outlier_count": c.get("outlier_count", ""),
                    "outlier_pct": c.get("outlier_pct", ""),
                    "detected_pattern": c.get("detected_pattern") or "",
                    "pii_detected": c.get("pii_detected") or "",
                    "min_length": c.get("min_length", ""),
                    "max_length": c.get("max_length", ""),
                    "avg_length": c.get("avg_length", ""),
                    "min_date": c.get("min_date", ""),
                    "max_date": c.get("max_date", ""),
                    "is_constant": c.get("is_constant", False),
                    "is_unique": c.get("is_unique", False),
                    "top_values_json": str(c.get("top_values") or ""),
                }
            )
    _w(
        csv_dir / "profiling.csv",
        ["table", "column", "dtype", "null_count", "null_pct", "distinct_count",
         "distinct_pct", "min_val", "max_val", "mean_val", "median_val", "std_val",
         "q25", "q75", "outlier_count", "outlier_pct", "detected_pattern",
         "pii_detected", "min_length", "max_length", "avg_length",
         "min_date", "max_date", "is_constant", "is_unique", "top_values_json"],
        prof_rows,
    )

    # ── relationships.csv ──────────────────────────────────────────────
    rel_rows = []
    for t in tables:
        for fk in t.get("foreign_keys", []):
            src_cols = ",".join(fk.get("constrained_columns", []))
            tgt_cols = ",".join(fk.get("referred_columns", []))
            integrity = next(
                (
                    fi
                    for fi in t.get("fk_integrity", [])
                    if fi.get("local_col") in fk.get("constrained_columns", [])
                ),
                {},
            )
            rel_rows.append(
                {
                    "from_table": t["name"],
                    "from_columns": src_cols,
                    "to_table": fk.get("referred_table", ""),
                    "to_columns": tgt_cols,
                    "fk_name": fk.get("name") or "",
                    "cardinality": fk.get("cardinality", "N:1"),
                    "orphan_count": integrity.get("orphan_count", ""),
                    "integrity_status": integrity.get("status", ""),
                }
            )
    _w(
        csv_dir / "relationships.csv",
        ["from_table", "from_columns", "to_table", "to_columns",
         "fk_name", "cardinality", "orphan_count", "integrity_status"],
        rel_rows,
    )

    # ── issues.csv ─────────────────────────────────────────────────────
    issue_rows = []
    for pt in prof_tables:
        for iss in pt.get("issues", []):
            issue_rows.append({"table": pt["name"], "issue": iss, "category": "profiling"})
    for t in tables:
        for fk in t.get("fk_integrity", []):
            if fk.get("status") == "violation":
                issue_rows.append(
                    {
                        "table": t["name"],
                        "issue": f"FK violation: {fk['local_col']} → {fk['ref_table']}.{fk['ref_col']} ({fk['orphan_count']} orphans)",
                        "category": "referential_integrity",
                    }
                )
    _w(csv_dir / "issues.csv", ["table", "issue", "category"], issue_rows)

    # ── pii_report.csv ─────────────────────────────────────────────────
    pii_rows = []
    for t in tables:
        for c in t.get("columns", []):
            pii_name = c.get("pii_type")
            pii_val = c.get("pii_detected")
            if pii_name or pii_val:
                pii_rows.append(
                    {
                        "table": t["name"],
                        "column": c["name"],
                        "type": c.get("type", ""),
                        "pii_from_name": pii_name or "",
                        "pii_from_values": pii_val or "",
                        "pii_type": pii_name or pii_val or "",
                    }
                )
    _w(
        csv_dir / "pii_report.csv",
        ["table", "column", "type", "pii_from_name", "pii_from_values", "pii_type"],
        pii_rows,
    )
