from __future__ import annotations

import datetime
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class LLMClient:
    """
    Multi-provider LLM client with disk caching.

    Providers: openai | anthropic | ollama | openai_compatible | none

    Both describe_table() and describe_columns() return (text, confidence) tuples
    where confidence is 0-100 (or None if the model didn't emit one).

    Column descriptions are enriched with profiling data (top values, ranges,
    cardinality, patterns, PII, lineage tags) when available.
    """

    def __init__(
        self,
        provider: str,
        model: str,
        cache_path: Optional[Path] = None,
        no_cache: bool = False,
        call_delay: float = 1.0,
        log_path: Optional[Path] = None,
        **kwargs,
    ):
        self.provider = provider.lower().strip()
        self.model = model.strip()
        self.no_cache = no_cache
        self.cache_path = cache_path
        self.kwargs = kwargs
        self._client = None
        self._cache: Dict[str, dict] = {}
        self._call_delay = call_delay        # minimum seconds between API calls
        self._last_call_time: float = 0.0   # monotonic timestamp of last call
        self._log_path = log_path            # JSONL log of every LLM call

        if self.is_enabled:
            self._load_cache()
            self._init_client()

    # ──────────────────────────────────────────────────────────────────────
    # Properties
    # ──────────────────────────────────────────────────────────────────────

    @property
    def is_enabled(self) -> bool:
        return self.provider != "none"

    # ──────────────────────────────────────────────────────────────────────
    # JSONL logging
    # ──────────────────────────────────────────────────────────────────────

    def _log(self, entry: dict) -> None:
        """Append one JSONL entry to the LLM calls log file (if configured)."""
        if not self._log_path:
            return
        entry.setdefault("ts", datetime.datetime.utcnow().isoformat())
        entry.setdefault("model", self.model)
        entry.setdefault("provider", self.provider)
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass  # never let logging errors disrupt the main flow

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def describe_table(
        self,
        table_name: str,
        columns,
        row_count: int,
        table_type: Optional[str] = None,
        table_profile=None,          # Optional[TableProfile]
    ) -> Tuple[str, Optional[int]]:
        """Return (description, confidence) for a table.

        Description is capped at 4 sentences.  Confidence is 0-100.
        """
        if not self.is_enabled:
            return "", None

        enriched = table_profile is not None
        cache_key = self._cache_key(table_name, [c.name for c in columns], enriched)
        if not self.no_cache and cache_key in self._cache:
            raw = self._cache[cache_key].get("table_description", "")
            if raw:  # Don't return empty cached results — allow retry
                return self._parse_confidence_suffix(raw)

        # ── Build context block ──────────────────────────────────────────
        ctx_lines = []
        actual_rows = (table_profile.row_count if table_profile else row_count) or 0
        ctx_lines.append(f"Row count: {actual_rows:,}")

        if table_type and table_type not in ("unknown", ""):
            ctx_lines.append(f"Classification: {table_type} table")

        if table_profile:
            if table_profile.null_pct > 0:
                ctx_lines.append(f"Overall null rate: {table_profile.null_pct:.1f}%")
            if table_profile.duplicate_pct > 1:
                ctx_lines.append(f"Duplicate rows: {table_profile.duplicate_pct:.1f}%")
            if table_profile.issues:
                ctx_lines.append(f"Data issues: {'; '.join(table_profile.issues[:3])}")

        ctx_block = "\n".join(ctx_lines)
        col_preview = ", ".join(
            f"{c.name}({c.type})" + (" PK" if c.is_primary_key else "")
            for c in columns[:30]
        )

        prompt = (
            f"You are a database documentation expert helping new engineers understand a production database.\n\n"
            f"Table: {table_name}\n"
            f"{ctx_block}\n"
            f"Columns: {col_preview}\n\n"
            f"Write a business description of what this table stores and its purpose. "
            f"Maximum 4 sentences. Be specific and concrete — do not just list column names.\n\n"
            f"On the very last line write ONLY the confidence bracket, nothing else:\n"
            f"[confidence] where confidence is 0-100.\n"
            f"Use the FULL range honestly:\n"
            f"  90-100 = crystal-clear purpose (e.g. 'orders', 'customers', 'invoices')\n"
            f"  70-89  = reasonable inference (name + columns make sense together)\n"
            f"  50-69  = educated guess (ambiguous table name, mixed columns)\n"
            f"  30-49  = vague (generic name like 'data', 'info', 'misc')\n"
            f"  0-29   = truly unknown\n"
            f"Example last line: [72]"
        )

        raw = self._generate(prompt, max_tokens=400)
        self._log({
            "type": "table_desc", "table": table_name,
            "response_len": len(raw), "parsed_ok": bool(raw.strip()),
            "response_preview": raw[:300],
        })
        if raw.strip():  # Only cache non-empty results so failures can be retried
            entry = self._cache.get(cache_key, {})
            entry["table_description"] = raw
            self._cache[cache_key] = entry
            self._save_cache()
        return self._parse_confidence_suffix(raw)

    def describe_columns(
        self,
        table_name: str,
        columns,
        col_profiles: Optional[Dict[str, Any]] = None,  # {col_name: ColumnProfile}
    ) -> Dict[str, Tuple[str, Optional[int]]]:
        """Return {column_name: (description, confidence)} for all columns.

        Handles partial caching: if a previous run described some columns but not
        all, only the missing ones are sent to the LLM. This avoids re-processing
        already-described columns and ensures no column is permanently skipped.

        Confidence is 0-100 per column.
        """
        if not self.is_enabled or not columns:
            return {}

        enriched = col_profiles is not None
        cache_key = self._cache_key(table_name, [c.name for c in columns], enriched)

        # ── Partial-cache resume ─────────────────────────────────────────
        # Preload any descriptions from a prior (possibly partial) run.
        # Only short-circuit if ALL columns are already covered.
        result_raw: Dict[str, str] = {}
        if not self.no_cache and cache_key in self._cache:
            cached = self._cache[cache_key].get("column_descriptions") or {}
            if cached:
                col_names_set = {c.name for c in columns}
                if col_names_set.issubset(set(cached.keys())):
                    # Fully cached — nothing to do
                    return {k: self._parse_confidence_suffix(v) for k, v in cached.items()}
                # Partially cached — seed result_raw and continue for the rest
                result_raw.update(cached)

        # Only send the LLM columns we don't already have
        remaining = [c for c in columns if c.name not in result_raw]
        if not remaining:
            return {k: self._parse_confidence_suffix(v) for k, v in result_raw.items()}

        # ── Build prompt for remaining columns ───────────────────────────
        lines = self._build_column_lines(remaining, col_profiles or {})
        col_block = "\n".join(lines)

        if enriched:
            instruction = (
                "For each column write ONE sentence describing its business meaning. "
                "Use the value ranges, distinct counts, sample values, and patterns as clues — "
                "they reveal the real business semantics. "
                "For enum-like columns (few distinct values), mention the key values. "
                "For ID/FK columns, explain what entity they identify or link to. "
                "For date columns, mention the time context. "
                "Avoid vague phrases like 'stores the X of the Y' — be concrete."
            )
        else:
            instruction = (
                "For each column write a one-sentence business description. "
                "Be specific about business meaning."
            )

        col_names = [c.name for c in remaining]
        prompt = (
            f"You are a database documentation expert helping new engineers "
            f"understand a production database.\n\n"
            f"Table: '{table_name}'\n"
            f"Columns:\n{col_block}\n\n"
            f"{instruction}\n\n"
            f"Rules:\n"
            f"- Respond ONLY with a valid JSON object, no other text.\n"
            f"- Each key is a column name, each value is a string.\n"
            f"- Append [N] at the end of each value where N is your confidence 0-100.\n"
            f"  Use the FULL range honestly:\n"
            f"  95-100 = obvious (id, name, created_at, email)\n"
            f"  70-89  = clear meaning from name + type\n"
            f"  50-69  = inferred from context or data patterns\n"
            f"  30-49  = ambiguous (could mean multiple things)\n"
            f"  0-29   = truly unknown\n"
            f"- Example: {{\"CustomerId\": \"Unique identifier for each customer [97]\", "
            f"\"UnitPrice\": \"Sale price per unit of the track [82]\", "
            f"\"Bytes\": \"File size of the audio track in bytes [91]\"}}\n"
            f"- Include ALL these columns: {', '.join(col_names)}"
        )

        # ── Batch LLM call ───────────────────────────────────────────────
        max_tok = min(4096, 150 * len(remaining) + 500)
        raw = self._generate(prompt, max_tokens=max_tok)
        raw_dict = self._extract_json_robust(raw)

        self._log({
            "type": "col_batch", "table": table_name,
            "columns_sent": len(remaining), "max_tokens": max_tok,
            "response_len": len(raw), "json_ok": bool(raw_dict),
            "parsed_count": len(raw_dict),
            "response_preview": raw[:500],
        })

        # Normalise keys: map model's key to canonical column name.
        # Skip any key the model invented that doesn't match a real column.
        col_name_lower = {c.name.lower(): c.name for c in remaining}
        for k, v in raw_dict.items():
            canonical = col_name_lower.get(k.lower())
            if canonical is None:
                continue  # model used an unknown key — ignore it
            if isinstance(v, str):
                result_raw[canonical] = v
            elif isinstance(v, dict):
                # Model returned {"description": "...", "confidence": 85} nested object
                desc = str(v.get("description", v.get("d", "")))
                conf = v.get("confidence", v.get("c", ""))
                result_raw[canonical] = f"{desc} [{conf}]" if conf else desc

        # ── Per-column fallback for still-missing columns ────────────────
        missing = [c for c in remaining if c.name not in result_raw]
        if missing:
            for c in missing[:20]:  # cap to avoid excessive API usage
                p = (col_profiles or {}).get(c.name)
                hints = self._profile_hints(p) if p else []
                hint_str = " | ".join(hints[:4]) if hints else ""
                single_prompt = (
                    f"Database table '{table_name}', column '{c.name}' [{c.type}]"
                    + (f". Context: {hint_str}" if hint_str else "")
                    + f".\nWrite exactly ONE sentence describing this column's business meaning, "
                    f"then append [N] where N is 0-100 confidence "
                    f"(95+=obvious, 70-89=clear, 50-69=inferred, 30-49=ambiguous, 0-29=unknown). "
                    f"Example: \"Unique customer identifier [95]\""
                )
                single_raw = self._generate(single_prompt, max_tokens=120)
                self._log({
                    "type": "col_fallback", "table": table_name, "col": c.name,
                    "response_preview": single_raw[:200],
                    "parsed_ok": bool(single_raw.strip()),
                })
                if single_raw.strip():
                    result_raw[c.name] = single_raw.strip()

        # ── Cache partial or full results ────────────────────────────────
        if result_raw:  # only persist when we have at least something
            entry = self._cache.get(cache_key, {})
            entry["column_descriptions"] = result_raw
            self._cache[cache_key] = entry
            self._save_cache()
        return {k: self._parse_confidence_suffix(v) for k, v in result_raw.items()}

    # ──────────────────────────────────────────────────────────────────────
    # Confidence parsing
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_confidence_suffix(text: str) -> Tuple[str, Optional[int]]:
        """Extract trailing [NN] confidence tag from an LLM string.

        'Great description [85]'  →  ('Great description', 85)
        'Just text'               →  ('Just text', None)
        'Confidence: 72\\n'       →  ('', 72)
        """
        if not text:
            return "", None
        text = text.strip()

        # Pattern 1: ends with [85] or similar
        m = re.search(r'^(.*?)\s*\[(\d{1,3})\]\s*$', text, re.DOTALL)
        if m:
            conf = min(100, max(0, int(m.group(2))))
            return m.group(1).strip(), conf

        # Pattern 2: "Confidence: 85" anywhere (table fallback)
        m2 = re.search(r'\bconfidence[:\s]+(\d{1,3})\b', text, re.IGNORECASE)
        if m2:
            conf = min(100, max(0, int(m2.group(1))))
            desc = re.sub(
                r'\n?\s*confidence[:\s]+\d+\s*\.?', '', text, flags=re.IGNORECASE
            ).strip()
            return desc, conf

        return text, None

    # ──────────────────────────────────────────────────────────────────────
    # Column line builder
    # ──────────────────────────────────────────────────────────────────────

    def _build_column_lines(self, columns, col_profiles: dict) -> List[str]:
        """Build one rich text line per column for the LLM prompt."""
        lines = []
        for c in columns:
            parts = [f"- {c.name} [{c.type}]"]

            flags = []
            if c.is_primary_key:
                flags.append("PK")
            if c.is_foreign_key and c.foreign_key_to:
                flags.append(f"FK→{c.foreign_key_to}")
            if not c.nullable:
                flags.append("required")
            if flags:
                parts.append(f"({', '.join(flags)})")

            p = col_profiles.get(c.name)
            if p is not None:
                hints = self._profile_hints(p)
                if hints:
                    parts.append("|")
                    parts.append(" | ".join(hints))

            if c.pii_type:
                parts.append(f"| PII:{c.pii_type}")
            if c.lineage_tags:
                parts.append(f"| tags:{','.join(c.lineage_tags)}")
            if c.comment:
                parts.append(f'| db-comment:"{c.comment}"')

            lines.append(" ".join(parts))
        return lines

    @staticmethod
    def _profile_hints(p) -> List[str]:
        """Extract meaningful hints from a ColumnProfile for the LLM prompt."""
        hints = []

        if p.null_pct == 0:
            hints.append("always populated")
        elif p.null_pct >= 95:
            hints.append(f"{p.null_pct:.0f}% null (almost never set)")
        elif p.null_pct >= 50:
            hints.append(f"{p.null_pct:.0f}% null (often absent)")
        elif p.null_pct > 5:
            hints.append(f"{p.null_pct:.0f}% null")

        if p.is_constant and p.top_values:
            hints.append(f'constant: always "{p.top_values[0]["value"]}"')
        elif p.is_unique:
            hints.append("all values unique (identifier)")
        elif p.distinct_count is not None:
            if p.distinct_count <= 1:
                pass
            elif p.distinct_count <= 20 and p.top_values:
                vals = [str(v["value"]) for v in p.top_values[:15] if v["value"] is not None]
                if vals:
                    hints.append(
                        f"{p.distinct_count} distinct values: "
                        + ", ".join(f'"{v}"' for v in vals)
                    )
            elif p.distinct_count <= 200 and p.top_values:
                vals = [str(v["value"]) for v in p.top_values[:5] if v["value"] is not None]
                if vals:
                    hints.append(
                        f"{p.distinct_count} distinct (top: "
                        + ", ".join(f'"{v}"' for v in vals)
                        + ")"
                    )
            else:
                if p.distinct_pct is not None:
                    hints.append(f"{p.distinct_count:,} distinct ({p.distinct_pct:.0f}% unique)")

        if p.min_val is not None and p.max_val is not None:
            mn, mx = p.min_val, p.max_val
            if mn == int(mn) and mx == int(mx):
                hints.append(f"range: {int(mn)}–{int(mx)}")
            else:
                hints.append(f"range: {mn:.2f}–{mx:.2f}")

        if p.min_date and p.max_date:
            hints.append(f"date range: {p.min_date} → {p.max_date}")

        if p.min_length is not None and p.max_length is not None and p.max_length > 0:
            if p.min_length == p.max_length and p.min_length in (2, 3, 4):
                hints.append(f"fixed {p.min_length}-char code")
            elif p.max_length <= 5:
                hints.append(f"short code ({p.min_length}–{p.max_length} chars)")

        if p.detected_pattern:
            hints.append(f"pattern: {p.detected_pattern}")
        if p.pii_detected:
            hints.append(f"contains: {p.pii_detected}")

        return hints

    # ──────────────────────────────────────────────────────────────────────
    # Core generation
    # ──────────────────────────────────────────────────────────────────────

    def _generate(self, prompt: str, max_tokens: int = 1024) -> str:
        # Enforce minimum inter-call delay to avoid rate limits
        if self._call_delay > 0:
            elapsed = time.monotonic() - self._last_call_time
            if elapsed < self._call_delay:
                time.sleep(self._call_delay - elapsed)

        for attempt in range(3):
            try:
                self._last_call_time = time.monotonic()
                if self.provider in ("openai", "openai_compatible", "openrouter"):
                    return self._openai(prompt, max_tokens)
                elif self.provider == "anthropic":
                    return self._anthropic(prompt, max_tokens)
                elif self.provider == "ollama":
                    return self._ollama(prompt, max_tokens)
                return ""
            except Exception as exc:
                exc_str = str(exc)
                err_lower = exc_str.lower()
                is_rate_limit = any(k in err_lower for k in ("rate", "429", "quota", "too many"))
                self._log({
                    "type": "error", "error": exc_str,
                    "is_rate_limit": is_rate_limit, "attempt": attempt,
                })
                if is_rate_limit and attempt < 2:
                    # Parse exact reset time from headers; auto-tune _call_delay for future calls
                    wait_secs = self._parse_rate_limit_wait(exc_str)
                    new_delay = self._detect_rate_limit_delay(exc_str)
                    if new_delay and new_delay > self._call_delay:
                        self._call_delay = new_delay
                        print(f"\n  [info] Rate limit detected ({new_delay * 60 / 60:.0f}/min) "
                              f"→ auto-adjusting delay to {new_delay:.1f}s. "
                              f"Set LLM_CALL_DELAY={new_delay:.0f} in .env to persist.")
                    print(f"\n  [warn] Rate limited — waiting {wait_secs:.0f}s until quota resets "
                          f"(retry {attempt + 2}/3)…")
                    self._log({"type": "rate_limit_wait", "wait_secs": wait_secs,
                               "new_call_delay": self._call_delay})
                    time.sleep(wait_secs)
                    continue
                print(f"  [warn] LLM call failed: {exc}")
                return ""
        return ""

    @staticmethod
    def _parse_rate_limit_wait(error_str: str) -> float:
        """Calculate how long to wait from a 429 error response.

        Parses X-RateLimit-Reset (Unix timestamp in milliseconds) when present —
        this is the exact moment the quota window resets.  Falls back to 65s
        (slightly over a minute, safe for per-minute limits).
        """
        # X-RateLimit-Reset: Unix timestamp in ms  (OpenRouter / OpenAI format)
        m = re.search(r"X-RateLimit-Reset['\"]:\s*['\"]?(\d{10,13})", error_str)
        if m:
            reset_ms = int(m.group(1))
            now_ms = time.time() * 1000
            wait_ms = reset_ms - now_ms
            if 0 < wait_ms < 300_000:           # sanity: 0 – 5 minutes
                return max(2.0, wait_ms / 1000 + 2.0)   # +2 s buffer

        # Retry-After header (seconds)
        m2 = re.search(r'retry.?after["\']:\s*["\']?(\d+)', error_str, re.IGNORECASE)
        if m2:
            return float(m2.group(1)) + 2.0

        return 65.0   # default: wait just over a minute

    @staticmethod
    def _detect_rate_limit_delay(error_str: str) -> Optional[float]:
        """Return the recommended _call_delay in seconds based on X-RateLimit-Limit header.

        e.g. X-RateLimit-Limit: 16  →  60/16 + 0.5  ≈  4.25 s/call
        """
        m = re.search(r"X-RateLimit-Limit['\"]:\s*['\"]?(\d+)", error_str)
        if m:
            calls_per_minute = int(m.group(1))
            if calls_per_minute > 0:
                return round(60.0 / calls_per_minute + 0.5, 1)
        return None

    def _openai(self, prompt: str, max_tokens: int) -> str:
        resp = self._client.chat.completions.create(
            model=self.model or "gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.2,
        )
        return resp.choices[0].message.content or ""

    def _anthropic(self, prompt: str, max_tokens: int) -> str:
        resp = self._client.messages.create(
            model=self.model or "claude-3-5-haiku-20241022",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text or ""

    def _ollama(self, prompt: str, max_tokens: int) -> str:
        import requests

        base_url = self.kwargs.get("base_url", "http://localhost:11434")
        resp = requests.post(
            f"{base_url}/api/generate",
            json={
                "model": self.model or "llama3.2",
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens},
            },
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")

    # ──────────────────────────────────────────────────────────────────────
    # Client init
    # ──────────────────────────────────────────────────────────────────────

    def _init_client(self) -> None:
        if self.provider == "openai":
            from openai import OpenAI
            self._client = OpenAI(api_key=self.kwargs.get("api_key"))

        elif self.provider == "anthropic":
            from anthropic import Anthropic
            self._client = Anthropic(api_key=self.kwargs.get("api_key"))

        elif self.provider == "openai_compatible":
            from openai import OpenAI
            self._client = OpenAI(
                api_key=self.kwargs.get("api_key", "not-needed"),
                base_url=self.kwargs.get("base_url"),
            )

        elif self.provider == "openrouter":
            from openai import OpenAI
            self._client = OpenAI(
                api_key=self.kwargs.get("api_key", ""),
                base_url="https://openrouter.ai/api/v1",
                default_headers={
                    "HTTP-Referer": "https://github.com/db-data-dict",
                    "X-Title": "db-data-dict",
                },
            )

        # ollama uses requests directly — no client object needed

    # ──────────────────────────────────────────────────────────────────────
    # Cache helpers
    # ──────────────────────────────────────────────────────────────────────

    def _load_cache(self) -> None:
        if self.cache_path and self.cache_path.exists():
            try:
                with open(self.cache_path) as f:
                    self._cache = json.load(f)
            except Exception:
                self._cache = {}

    def _save_cache(self) -> None:
        if self.cache_path:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, "w") as f:
                json.dump(self._cache, f, indent=2)

    @staticmethod
    def _cache_key(table_name: str, col_names: List[str], enriched: bool = False) -> str:
        prefix = "enriched:" if enriched else "basic:"
        raw = f"{prefix}{table_name}:{','.join(sorted(col_names))}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    # ──────────────────────────────────────────────────────────────────────
    # Robust JSON extraction
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_json_robust(text: str) -> Dict[str, Any]:
        """Extract a JSON object from LLM output, tolerating common model errors."""
        if not text:
            return {}

        # 1. Strip markdown code fences
        text = re.sub(r'^```(?:json)?\s*', '', text.strip(), flags=re.MULTILINE)
        text = re.sub(r'\s*```$', '', text.strip(), flags=re.MULTILINE)
        text = text.strip()

        # 2. Direct parse (fastest path)
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

        # 3. Find outermost { ... } block
        start = text.find('{')
        end   = text.rfind('}')
        if start != -1 and end > start:
            candidate = text[start:end + 1]
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

            # 4. Fix trailing commas
            fixed = re.sub(r',\s*([}\]])', r'\1', candidate)
            try:
                obj = json.loads(fixed)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass

            # 5. Recover key:value pairs with regex (handles truncated JSON)
            partial: Dict[str, Any] = {}
            for m in re.finditer(r'"([^"]+)"\s*:\s*"((?:[^"\\]|\\.)*)"', candidate):
                partial[m.group(1)] = m.group(2)
            if partial:
                return partial

        return {}

    # Backwards-compat alias
    @staticmethod
    def _extract_json(text: str) -> Dict[str, str]:
        return LLMClient._extract_json_robust(text)
