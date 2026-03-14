"""
Infer implicit foreign-key relationships from column naming conventions.

Only applied to tables that have NO declared FK constraints — databases that
define FK constraints explicitly take priority and are never touched here.

Heuristic: a column named  <candidate>_id  (or  <candidate>id)  is treated
as an FK to table <candidate> (or a simple plural/singular variant) when such
a table exists in the schema.

Inferred FKs are flagged with  ForeignKeyInfo.inferred = True  so the ERD
can render them differently (dashed line) from declared constraints.
"""
from __future__ import annotations

from typing import List

from ..db.inspector import ForeignKeyInfo, TableInfo


# ---------------------------------------------------------------------------
# Simple singular/plural helpers (good enough for typical DB naming)
# ---------------------------------------------------------------------------

def _variants(word: str) -> List[str]:
    """Return likely table-name variants for a column-name prefix."""
    candidates = {word}
    # plural → singular
    if word.endswith("ies"):
        candidates.add(word[:-3] + "y")
    elif word.endswith("ses") or word.endswith("xes") or word.endswith("zes"):
        candidates.add(word[:-2])
    elif word.endswith("s") and not word.endswith("ss"):
        candidates.add(word[:-1])
    # singular → plural
    candidates.add(word + "s")
    if word.endswith("y"):
        candidates.add(word[:-1] + "ies")
    return candidates


def infer_implicit_fks(tables_info: List[TableInfo]) -> None:
    """
    For every table that has *no* declared FK constraints, attempt to infer
    relationships from column names ending in ``_id`` or ``id``.

    Results are appended to  table.foreign_keys  and the matching column has
    ``is_foreign_key`` / ``foreign_key_to``  set in place.
    """
    # Build a lowercase lookup: lower_name → actual TableInfo
    table_map = {t.name.lower(): t for t in tables_info}

    for table in tables_info:
        # Only run heuristics when there are NO declared FK constraints
        if table.foreign_keys:
            continue

        for col in table.columns:
            # Skip PKs and columns that already have an FK
            if col.is_primary_key or col.is_foreign_key:
                continue

            col_lower = col.name.lower()

            # Extract the prefix before _id / id suffix
            if col_lower.endswith("_id"):
                prefix = col_lower[:-3]
            elif col_lower.endswith("id") and len(col_lower) > 2:
                prefix = col_lower[:-2]
            else:
                continue

            if not prefix:
                continue

            # Try the prefix and its plural/singular variants
            ref_table_info: TableInfo | None = None
            for variant in _variants(prefix):
                if variant in table_map and table_map[variant].name != table.name:
                    ref_table_info = table_map[variant]
                    break

            if ref_table_info is None:
                continue

            # Use the referenced table's first PK column, fall back to "id"
            ref_pk = (ref_table_info.primary_keys[0]
                      if ref_table_info.primary_keys else "id")

            fk = ForeignKeyInfo(
                name=f"inferred__{table.name}__{col.name}",
                constrained_columns=[col.name],
                referred_schema=None,
                referred_table=ref_table_info.name,
                referred_columns=[ref_pk],
                inferred=True,
            )
            table.foreign_keys.append(fk)
            col.is_foreign_key = True
            col.foreign_key_to = f"{ref_table_info.name}.{ref_pk}"
