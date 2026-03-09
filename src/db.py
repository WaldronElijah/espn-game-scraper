"""
PostgreSQL integration for the nba_games table.

This module is intentionally small and focused:
- get_connection(): create a psycopg connection using DATABASE_URL
- insert_game(): upsert one game's data into nba_games
"""

from __future__ import annotations

import os
from typing import Any, Mapping

import psycopg
from psycopg.types.json import Json
from dotenv import load_dotenv


# Load environment variables from .env so you don't hardcode credentials.
load_dotenv()


def get_connection() -> psycopg.Connection:
    """
    Create a PostgreSQL connection using the DATABASE_URL environment variable.

    Example .env entry:
        DATABASE_URL=postgresql://user:password@localhost:5432/your_db_name
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Create a .env file with DATABASE_URL=postgresql://..."
        )

    return psycopg.connect(dsn)


def insert_game(game_data: Mapping[str, Any]) -> None:
    """
    Insert or update a single game row in the nba_games table.

    - Uses parameterized SQL for safety.
    - ON CONFLICT(game_id) DO UPDATE makes this operation idempotent:
      you can safely call insert_game() multiple times for the same game.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO nba_games (
                    game_id,
                    "date",
                    away_team,
                    home_team,
                    away_score,
                    home_score,
                    game_status,
                    location,
                    referees,
                    opening_spread,
                    opening_total,
                    draftkings_lines
                ) VALUES (
                    %(game_id)s,
                    %(date)s,
                    %(away_team)s,
                    %(home_team)s,
                    %(away_score)s,
                    %(home_score)s,
                    %(game_status)s,
                    %(location)s,
                    %(referees)s,
                    %(opening_spread)s,
                    %(opening_total)s,
                    %(draftkings_lines)s
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    "date"           = EXCLUDED."date",
                    away_team        = EXCLUDED.away_team,
                    home_team        = EXCLUDED.home_team,
                    away_score       = EXCLUDED.away_score,
                    home_score       = EXCLUDED.home_score,
                    game_status      = EXCLUDED.game_status,
                    location         = EXCLUDED.location,
                    referees         = EXCLUDED.referees,
                    opening_spread   = EXCLUDED.opening_spread,
                    opening_total    = EXCLUDED.opening_total,
                    draftkings_lines = EXCLUDED.draftkings_lines;
                """,
                {
                    "game_id": game_data["game_id"],
                    "date": game_data.get("date"),
                    "away_team": game_data.get("away_team"),
                    "home_team": game_data.get("home_team"),
                    "away_score": game_data.get("away_score"),
                    "home_score": game_data.get("home_score"),
                    "game_status": game_data.get("game_status"),
                    "location": game_data.get("location"),
                    # psycopg will map Python lists to PostgreSQL arrays automatically.
                    "referees": game_data.get("referees"),
                    "opening_spread": game_data.get("opening_spread"),
                    "opening_total": game_data.get("opening_total"),
                    # Wrap dict as Json so psycopg sends it as JSON, not a string.
                    "draftkings_lines": Json(game_data.get("draftkings_lines")),
                },
            )

