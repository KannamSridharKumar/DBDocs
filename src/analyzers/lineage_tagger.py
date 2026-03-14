"""Tag columns with semantic lineage hints based on column names."""
from __future__ import annotations

import re
from typing import List, Tuple

# (name_regex, tags_list)
RULES: List[Tuple[str, List[str]]] = [
    # Created timestamp
    (r"^created_at$|^create_date$|^created_date$|^insert_date$|^inserted_at$|^creation_date$", ["temporal", "audit", "created_at"]),
    # Updated timestamp
    (r"^updated_at$|^modified_at$|^last_modified$|^update_date$|^last_updated$|^modified_date$", ["temporal", "audit", "updated_at"]),
    # Deleted timestamp → soft-delete marker
    (r"^deleted_at$|^delete_date$|^removed_at$|^archived_at$", ["temporal", "audit", "soft_delete"]),
    # Created / updated by (user)
    (r"^created_by$|^inserted_by$|^added_by$", ["audit", "created_by"]),
    (r"^updated_by$|^modified_by$|^last_modified_by$|^changed_by$", ["audit", "updated_by"]),
    (r"^deleted_by$|^removed_by$", ["audit", "deleted_by"]),
    # Soft-delete boolean
    (r"^is_deleted$|^deleted$|^is_removed$|^is_archived$", ["soft_delete"]),
    (r"^is_active$|^active$|^is_enabled$|^enabled$|^is_visible$", ["status", "soft_delete"]),
    # Status / state
    (r"^status$|^state$|^stage$|^phase$|^workflow_state$", ["status"]),
    # Optimistic locking / versioning
    (r"^version$|^row_version$|^etag$|^revision$|^seq_no$|^sequence_no$", ["versioning"]),
    # Surrogate / primary key
    (r"^id$|_id$", ["surrogate_key"]),
    # Natural / business key
    (r"^code$|_code$|^sku$|^uuid$|^guid$|^external_id$|^reference_id$|^business_key$", ["natural_key"]),
    # Financial
    (r"amount|price|cost|fee\b|tax\b|discount|total\b|subtotal|revenue|balance|charge", ["financial"]),
    # Geographic
    (r"latitude|longitude|\blat\b|\blon\b|\blng\b|coordinate|geo_|_geo", ["geographic"]),
    # Temporal (generic date/time not covered above)
    (r"_date$|_at$|_time$|_datetime$|_timestamp$", ["temporal"]),
    # Flag / boolean
    (r"^is_|^has_|^can_|^should_|^flag_|_flag$|_yn$", ["boolean_flag"]),
]

_COMPILED: List[Tuple[re.Pattern, List[str]]] = [
    (re.compile(pattern, re.IGNORECASE), tags)
    for pattern, tags in RULES
]


def get_tags(col_name: str) -> List[str]:
    tags: List[str] = []
    for pattern, tag_list in _COMPILED:
        if pattern.search(col_name):
            for t in tag_list:
                if t not in tags:
                    tags.append(t)
    return tags


def tag_tables(tables_info: list) -> None:
    """Enrich ColumnInfo.lineage_tags in-place."""
    for t in tables_info:
        for col in t.columns:
            col.lineage_tags = get_tags(col.name)
