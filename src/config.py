from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    # Database
    db_url: str = ""
    db_schema: Optional[str] = None
    table_filter: Optional[str] = None

    # LLM
    llm_provider: str = "none"  # openai | anthropic | ollama | openai_compatible | openrouter | none
    llm_model: str = ""
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    ollama_base_url: str = "http://localhost:11434"
    openai_compatible_base_url: Optional[str] = None
    openai_compatible_api_key: Optional[str] = None
    openrouter_api_key: Optional[str] = None

    # Output
    output_dir: str = "./output"
    export_formats: str = "html"      # html | json | csv | all (comma-separated)

    # Profiling
    max_sample_rows: int = 100000
    top_values_count: int = 10
    sample_data_rows: int = 5

    # Rate limiting
    llm_call_delay: float = 1.0   # minimum seconds between LLM API calls


def load_config() -> Config:
    """Load configuration from environment variables / .env file."""
    return Config(
        db_url=os.getenv("DB_URL", ""),
        db_schema=os.getenv("DB_SCHEMA") or None,
        table_filter=os.getenv("TABLE_FILTER") or None,
        llm_provider=os.getenv("LLM_PROVIDER", "none").lower().strip(),
        llm_model=os.getenv("LLM_MODEL", "").strip(),
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY") or None,
        ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        openai_compatible_base_url=os.getenv("OPENAI_COMPATIBLE_BASE_URL") or None,
        openai_compatible_api_key=os.getenv("OPENAI_COMPATIBLE_API_KEY") or None,
        openrouter_api_key=os.getenv("OPENROUTER_API_KEY") or None,
        output_dir=os.getenv("OUTPUT_DIR", "./output"),
        export_formats=os.getenv("EXPORT", "html"),
        max_sample_rows=int(os.getenv("MAX_SAMPLE_ROWS", "100000")),
        top_values_count=int(os.getenv("TOP_VALUES_COUNT", "10")),
        sample_data_rows=int(os.getenv("SAMPLE_DATA_ROWS", "5")),
        llm_call_delay=float(os.getenv("LLM_CALL_DELAY", "1.0")),
    )
