"""Detect naming convention violations across columns in a table."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple


def _detect_style(name: str) -> Optional[str]:
    if re.fullmatch(r"[a-z][a-z0-9]*(_[a-z0-9]+)*", name):
        return "snake_case"
    if re.fullmatch(r"[a-z][a-zA-Z0-9]+", name):
        return "camelCase"
    if re.fullmatch(r"[A-Z][a-zA-Z0-9]+", name):
        return "PascalCase"
    if re.fullmatch(r"[A-Z][A-Z0-9]*(_[A-Z0-9]+)*", name):
        return "UPPER_SNAKE"
    return None


def analyze_table(columns) -> Dict[str, str]:
    """Return {col_name: issue_message} for columns with naming issues."""
    styles = [(c.name, _detect_style(c.name)) for c in columns]

    # Count dominant style
    counts: Dict[str, int] = {}
    for _, s in styles:
        if s:
            counts[s] = counts.get(s, 0) + 1

    if not counts:
        return {}

    dominant = max(counts, key=counts.get)
    issues: Dict[str, str] = {}

    for col_name, style in styles:
        if style is None:
            issues[col_name] = "Non-standard naming (mixed/special chars)"
        elif style != dominant:
            issues[col_name] = f"Expected {dominant}, found {style}"

    return issues


def analyze_tables(tables_info: list) -> None:
    """Set ColumnInfo.naming_issue in-place for each table."""
    for t in tables_info:
        issues = analyze_table(t.columns)
        for col in t.columns:
            col.naming_issue = issues.get(col.name)
