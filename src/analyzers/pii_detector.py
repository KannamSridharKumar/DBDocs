"""PII detection from column names and sampled values."""
from __future__ import annotations

import re
from typing import List, Optional, Tuple

# (regex_on_col_name, pii_type)
NAME_PATTERNS: List[Tuple[str, str]] = [
    (r"email|e_mail|emailaddress", "Email"),
    (r"phone|mobile|cell_no|telephone|fax", "Phone"),
    (r"ssn|social_security|sin\b", "SSN"),
    (r"passport", "Passport"),
    (r"credit_card|card_number|cardnum|\bpan\b|card_no", "Credit Card"),
    (r"\baddress\b|street|addr\b", "Address"),
    (r"birth_date|dob\b|date_of_birth|birthdate", "Date of Birth"),
    (r"ip_address|ip_addr|ipaddr|\bip\b", "IP Address"),
    (r"first_name|last_name|full_name|fname|lname|surname|forename|given_name", "Person Name"),
    (r"salary|income|wage\b|compensation\b", "Financial"),
    (r"national_id|nationalid|\bnid\b|\bnic\b", "National ID"),
    (r"password|passwd|_pwd\b", "Password"),
    (r"tax_id|taxid|\bein\b|\bvat\b", "Tax ID"),
    (r"gender|sex\b", "Demographic"),
    (r"race|ethnicity|religion|nationality", "Demographic"),
    (r"location|latitude|longitude|\blat\b|\blon\b|\blng\b", "Location"),
]

# Value-level patterns (compiled)
VALUE_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"), "Email"),
    (re.compile(r"^\d{3}-\d{2}-\d{4}$"), "SSN"),
    (re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), "IP Address"),
    (re.compile(r"^\+?[\d\s\-\(\)\.]{7,20}$"), "Phone"),
    (re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"), "UUID"),
]
_MATCH_THRESHOLD = 0.65


def detect_from_name(col_name: str) -> Optional[str]:
    lower = col_name.lower()
    for pattern, pii_type in NAME_PATTERNS:
        if re.search(pattern, lower):
            return pii_type
    return None


def detect_from_values(values: List) -> Optional[str]:
    sample = [str(v) for v in values if v is not None][:30]
    if not sample:
        return None
    for compiled, pii_type in VALUE_PATTERNS:
        hits = sum(1 for v in sample if compiled.match(v))
        if hits / len(sample) >= _MATCH_THRESHOLD:
            return pii_type
    return None


def tag_tables(tables_info: list) -> None:
    """Enrich ColumnInfo.pii_type from column names (in-place)."""
    for t in tables_info:
        for col in t.columns:
            if not col.pii_type:
                col.pii_type = detect_from_name(col.name)
