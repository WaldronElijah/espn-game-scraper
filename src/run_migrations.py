"""
Run SQL migrations from Python (create table, then alter for cleaned columns).

Requires DATABASE_URL in .env. Run from project root: python -m src.run_migrations
"""
from __future__ import annotations

import pathlib

# Project root (parent of src/)
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent


def run_migrations() -> None:
    from src.db import get_connection

    for name in ("create_table.sql", "alter_nba_games_cleaned.sql"):
        path = PROJECT_ROOT / "sql" / name
        sql = path.read_text()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        print(f"Ran {name}")


if __name__ == "__main__":
    run_migrations()
