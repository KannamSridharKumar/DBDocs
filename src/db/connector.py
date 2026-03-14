from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def create_db_engine(db_url: str) -> Engine:
    """Create a SQLAlchemy engine and verify the connection."""
    # Strip whitespace and accidental quotes (common .env copy-paste mistakes)
    db_url = db_url.strip().strip('"').strip("'")

    # Catch Python f-string literals accidentally pasted into .env
    if db_url.startswith(("f'", 'f"')):
        raise ValueError(
            "DB_URL looks like a Python f-string (starts with f\" or f'). "
            "In .env files, write the URL directly without f\"...\" wrapping.\n"
            "  ✗  DB_URL=f\"postgresql://user:pass@host/db\"\n"
            "  ✓  DB_URL=postgresql://user:pass@host/db"
        )

    connect_args: dict = {}

    if db_url.startswith(("postgresql", "postgres")):
        connect_args = {"connect_timeout": 30}
    elif db_url.startswith("mysql"):
        connect_args = {"connect_timeout": 30}

    engine = create_engine(
        db_url,
        connect_args=connect_args,
        pool_pre_ping=True,
    )

    # Verify connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    return engine


def get_db_name(db_url: str) -> str:
    """Extract a human-readable database name from a connection URL."""
    try:
        if "sqlite" in db_url:
            path = db_url.split("///")[-1]
            return path.split("/")[-1].replace(".db", "") or "sqlite"
        # For all others: last segment before query string
        path_part = db_url.split("/")[-1].split("?")[0]
        return path_part or "database"
    except Exception:
        return "database"
