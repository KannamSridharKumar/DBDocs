from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from ..analyzers.pii_detector import detect_from_values

# ── Value-pattern detection ────────────────────────────────────────────
_PATTERNS = [
    (re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"), "UUID"),
    (re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"), "Email"),
    (re.compile(r"^https?://"), "URL"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?"), "Date String"),
    (re.compile(r"^\+?[\d\s\-\(\)\.]{7,20}$"), "Phone"),
    (re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), "IP Address"),
    (re.compile(r"^\{.*\}$|^\[.*\]$", re.DOTALL), "JSON"),
]
_PAT_THRESHOLD = 0.70
_BOOL_VALS = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}


def _detect_pattern(non_null: pd.Series) -> Optional[str]:
    sample = [str(v) for v in non_null.head(40)]
    if not sample:
        return None
    # Boolean string
    if set(str(v).lower() for v in sample) <= _BOOL_VALS:
        return "Boolean String"
    for compiled, name in _PATTERNS:
        hits = sum(1 for v in sample if compiled.match(v))
        if hits / len(sample) >= _PAT_THRESHOLD:
            return name
    return None


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_count: int
    null_pct: float
    distinct_count: int
    distinct_pct: float
    # Numeric
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    mean_val: Optional[float] = None
    median_val: Optional[float] = None
    std_val: Optional[float] = None
    q25: Optional[float] = None
    q75: Optional[float] = None
    histogram: Optional[List[Dict]] = None
    outlier_count: Optional[int] = None
    outlier_pct: Optional[float] = None
    # String
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    # Date
    min_date: Optional[str] = None
    max_date: Optional[str] = None
    # Categorical
    top_values: Optional[List[Dict]] = None
    # Semantic
    detected_pattern: Optional[str] = None
    pii_detected: Optional[str] = None   # from values
    is_constant: bool = False
    is_unique: bool = False


@dataclass
class TableProfile:
    name: str
    row_count: int
    sampled_rows: int
    column_count: int
    total_null_cells: int
    null_pct: float
    duplicate_count: int
    duplicate_pct: float
    quality_score: float
    columns: List[ColumnProfile]
    issues: List[str] = field(default_factory=list)
    trend_data: Optional[Dict] = None     # {column, by_period:[{period,count}]}
    fk_integrity: List[Dict] = field(default_factory=list)


class DataProfiler:
    def __init__(self, engine: Engine, max_sample_rows: int = 100_000, top_n: int = 10):
        self.engine = engine
        self.max_sample_rows = max_sample_rows
        self.top_n = top_n

    def profile_table(self, table_name: str, schema: Optional[str] = None) -> Optional[TableProfile]:
        try:
            return self._profile(table_name, schema)
        except Exception as exc:
            print(f"  [warn] Could not profile '{table_name}': {exc}")
            return None

    def profile_all(self, table_names, schema=None, progress_callback=None):
        results = []
        for i, name in enumerate(table_names):
            if progress_callback:
                progress_callback(i, len(table_names), name)
            p = self.profile_table(name, schema)
            if p:
                results.append(p)
        return results

    # ── Internal ──────────────────────────────────────────────────────

    def _profile(self, table_name: str, schema: Optional[str]) -> TableProfile:
        from sqlalchemy import text
        quoted = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

        with self.engine.connect() as conn:
            total_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {quoted}")).scalar() or 0)

        if total_rows == 0:
            return TableProfile(name=table_name, row_count=0, sampled_rows=0,
                                column_count=0, total_null_cells=0, null_pct=0,
                                duplicate_count=0, duplicate_pct=0, quality_score=100, columns=[])

        limit = (f" LIMIT {self.max_sample_rows}"
                 if self.max_sample_rows > 0 and total_rows > self.max_sample_rows else "")
        df = pd.read_sql(f"SELECT * FROM {quoted}{limit}", self.engine)
        sampled = len(df)

        try:
            dup_count = int(df.duplicated().sum())
            dup_pct = round(dup_count / sampled * 100, 2) if sampled else 0.0
        except Exception:
            dup_count, dup_pct = 0, 0.0

        col_profiles = [self._profile_column(df, col, sampled) for col in df.columns]
        total_null = sum(c.null_count for c in col_profiles)
        total_cells = sampled * len(df.columns) if df.columns.size else 1
        null_pct = round(total_null / total_cells * 100, 2) if total_cells else 0.0

        issues = self._detect_issues(col_profiles, dup_pct)
        trend = self._calc_trend(df, sampled < total_rows)

        return TableProfile(
            name=table_name, row_count=total_rows, sampled_rows=sampled,
            column_count=len(df.columns), total_null_cells=total_null,
            null_pct=null_pct, duplicate_count=dup_count, duplicate_pct=dup_pct,
            quality_score=self._quality_score(col_profiles, dup_pct),
            columns=col_profiles, issues=issues, trend_data=trend,
        )

    def _profile_column(self, df: pd.DataFrame, col_name: str, total: int) -> ColumnProfile:
        col = df[col_name]
        null_count = int(col.isna().sum())
        null_pct = round(null_count / total * 100, 2) if total else 0.0
        non_null = col.dropna()
        distinct = int(non_null.nunique()) if len(non_null) else 0
        distinct_pct = round(distinct / total * 100, 2) if total else 0.0

        profile = ColumnProfile(
            name=col_name, dtype=str(col.dtype),
            null_count=null_count, null_pct=null_pct,
            distinct_count=distinct, distinct_pct=distinct_pct,
            is_constant=(distinct == 1 and null_count < total),
            is_unique=(distinct == total and null_count == 0),
        )

        if pd.api.types.is_numeric_dtype(col):
            self._fill_numeric(profile, non_null, total)
        elif pd.api.types.is_datetime64_any_dtype(col):
            self._fill_datetime(profile, non_null, total)
        else:
            self._fill_string(profile, non_null, total)

        if profile.top_values is None and 0 < distinct <= 25 and len(non_null):
            profile.top_values = self._top_values(non_null, total)

        return profile

    def _fill_numeric(self, p: ColumnProfile, non_null: pd.Series, total: int) -> None:
        if not len(non_null):
            return
        try:
            p.min_val = self._sf(non_null.min())
            p.max_val = self._sf(non_null.max())
            p.mean_val = self._sf(non_null.mean())
            p.median_val = self._sf(non_null.median())
            p.std_val = self._sf(non_null.std())
            p.q25 = self._sf(non_null.quantile(0.25))
            p.q75 = self._sf(non_null.quantile(0.75))
            p.histogram = self._histogram(non_null)
            # Outlier detection (IQR method)
            q1, q3 = non_null.quantile(0.25), non_null.quantile(0.75)
            iqr = q3 - q1
            if iqr > 0:
                lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                outliers = non_null[(non_null < lo) | (non_null > hi)]
                p.outlier_count = int(len(outliers))
                p.outlier_pct = round(len(outliers) / total * 100, 2)
        except Exception:
            pass

    def _fill_datetime(self, p: ColumnProfile, non_null: pd.Series, total: int) -> None:
        if not len(non_null):
            return
        try:
            p.min_date = str(non_null.min())
            p.max_date = str(non_null.max())
            vc = non_null.dt.year.value_counts().head(self.top_n)
            p.top_values = [
                {"value": str(int(k)), "count": int(v), "pct": round(v / total * 100, 1)}
                for k, v in vc.items()
            ]
        except Exception:
            pass

    def _fill_string(self, p: ColumnProfile, non_null: pd.Series, total: int) -> None:
        if not len(non_null):
            return
        try:
            s = non_null.astype(str)
            lengths = s.str.len()
            p.min_length = int(lengths.min())
            p.max_length = int(lengths.max())
            p.avg_length = round(float(lengths.mean()), 1)
        except Exception:
            pass
        try:
            p.top_values = self._top_values(non_null, total)
        except Exception:
            pass
        # Pattern + PII detection from values
        try:
            p.detected_pattern = _detect_pattern(non_null)
        except Exception:
            pass
        try:
            p.pii_detected = detect_from_values(non_null.tolist())
        except Exception:
            pass

    def _top_values(self, non_null: pd.Series, total: int) -> List[Dict]:
        vc = non_null.value_counts().head(self.top_n)
        return [
            {"value": str(k)[:120], "count": int(v), "pct": round(v / total * 100, 1)}
            for k, v in vc.items()
        ]

    def _histogram(self, series: pd.Series, bins: int = 10) -> List[Dict]:
        try:
            counts, edges = np.histogram(series.dropna(), bins=bins)
            return [{"bin": f"{edges[i]:.2g}–{edges[i+1]:.2g}", "count": int(c)}
                    for i, c in enumerate(counts)]
        except Exception:
            return []

    def _calc_trend(self, df: pd.DataFrame, is_sampled: bool) -> Optional[Dict]:
        """Find first date/datetime column and compute monthly trend."""
        for col_name in df.columns:
            col = df[col_name]
            parsed = None
            if pd.api.types.is_datetime64_any_dtype(col):
                parsed = col
            else:
                # Try parsing string column as date
                sample = col.dropna().head(20).astype(str)
                if sample.str.match(r"\d{4}-\d{2}-\d{2}").mean() > 0.7:
                    try:
                        parsed = pd.to_datetime(col, errors="coerce")
                        if parsed.isna().mean() > 0.5:
                            parsed = None
                    except Exception:
                        parsed = None
            if parsed is not None and not parsed.dropna().empty:
                try:
                    by_period = (
                        parsed.dropna()
                        .dt.to_period("M")
                        .value_counts()
                        .sort_index()
                    )
                    return {
                        "column": col_name,
                        "is_sampled": is_sampled,
                        "by_period": [
                            {"period": str(k), "count": int(v)}
                            for k, v in by_period.items()
                        ],
                    }
                except Exception:
                    pass
        return None

    @staticmethod
    def _sf(v) -> Optional[float]:
        try:
            f = float(v)
            return round(f, 6) if not (np.isnan(f) or np.isinf(f)) else None
        except Exception:
            return None

    @staticmethod
    def _quality_score(cols: List[ColumnProfile], dup_pct: float) -> float:
        if not cols:
            return 100.0
        avg_null = sum(c.null_pct for c in cols) / len(cols)
        return round(max(0, min(100, 100 - avg_null * 0.5 - min(dup_pct * 0.3, 30))), 1)

    @staticmethod
    def _detect_issues(cols: List[ColumnProfile], dup_pct: float) -> List[str]:
        issues = []
        if dup_pct > 10:
            issues.append(f"High duplicate rate: {dup_pct:.1f}%")
        for c in cols:
            if c.null_pct > 80:
                issues.append(f"Column '{c.name}' is {c.null_pct:.0f}% null")
            elif c.null_pct > 50:
                issues.append(f"Column '{c.name}' has majority nulls ({c.null_pct:.0f}%)")
            if c.is_constant:
                issues.append(f"Column '{c.name}' has only one distinct value")
            if c.pii_detected:
                issues.append(f"Column '{c.name}' may contain PII ({c.pii_detected}) — verify")
        return issues
