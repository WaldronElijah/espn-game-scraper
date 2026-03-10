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


def insert_cleaned_game(cleaned: Mapping[str, Any]) -> None:
    """
    Insert or update a single game using the cleaned record shape.

    Uses the same nba_games table; expects keys from clean.clean_game_record()
    (start_time_utc, status, winner, loser, margin, total_points, venue_name,
    opening_spread_home, opening_spread_away, ats_winner, ou_result, etc.).
    Also backfills legacy columns (date, game_status, location, opening_spread)
    for backward compatibility.
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
                    draftkings_lines,
                    start_time_utc,
                    scraped_at_utc,
                    status,
                    winner,
                    loser,
                    margin,
                    total_points,
                    venue_name,
                    venue_city,
                    venue_state,
                    opening_spread_home,
                    opening_spread_away,
                    ats_winner,
                    ou_result
                ) VALUES (
                    %(game_id)s,
                    %(start_time_utc)s,
                    %(away_team)s,
                    %(home_team)s,
                    %(away_score)s,
                    %(home_score)s,
                    %(status)s,
                    %(venue_name)s,
                    %(referees)s,
                    %(opening_spread_home)s,
                    %(opening_total)s,
                    %(draftkings_lines)s,
                    %(start_time_utc)s,
                    %(scraped_at_utc)s,
                    %(status)s,
                    %(winner)s,
                    %(loser)s,
                    %(margin)s,
                    %(total_points)s,
                    %(venue_name)s,
                    %(venue_city)s,
                    %(venue_state)s,
                    %(opening_spread_home)s,
                    %(opening_spread_away)s,
                    %(ats_winner)s,
                    %(ou_result)s
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    "date"                 = EXCLUDED."date",
                    away_team              = EXCLUDED.away_team,
                    home_team              = EXCLUDED.home_team,
                    away_score             = EXCLUDED.away_score,
                    home_score             = EXCLUDED.home_score,
                    game_status            = EXCLUDED.game_status,
                    location               = EXCLUDED.location,
                    referees               = EXCLUDED.referees,
                    opening_spread         = EXCLUDED.opening_spread,
                    opening_total          = EXCLUDED.opening_total,
                    draftkings_lines       = EXCLUDED.draftkings_lines,
                    start_time_utc         = EXCLUDED.start_time_utc,
                    scraped_at_utc         = EXCLUDED.scraped_at_utc,
                    status                 = EXCLUDED.status,
                    winner                 = EXCLUDED.winner,
                    loser                  = EXCLUDED.loser,
                    margin                 = EXCLUDED.margin,
                    total_points           = EXCLUDED.total_points,
                    venue_name             = EXCLUDED.venue_name,
                    venue_city             = EXCLUDED.venue_city,
                    venue_state            = EXCLUDED.venue_state,
                    opening_spread_home    = EXCLUDED.opening_spread_home,
                    opening_spread_away    = EXCLUDED.opening_spread_away,
                    ats_winner             = EXCLUDED.ats_winner,
                    ou_result              = EXCLUDED.ou_result;
                """,
                {
                    "game_id": cleaned.get("game_id"),
                    "start_time_utc": cleaned.get("start_time_utc"),
                    "away_team": cleaned.get("away_team"),
                    "home_team": cleaned.get("home_team"),
                    "away_score": cleaned.get("away_score"),
                    "home_score": cleaned.get("home_score"),
                    "status": cleaned.get("status"),
                    "venue_name": cleaned.get("venue_name"),
                    "referees": cleaned.get("referees"),
                    "opening_spread_home": cleaned.get("opening_spread_home"),
                    "opening_total": cleaned.get("opening_total"),
                    "draftkings_lines": Json(cleaned.get("draftkings_lines")),
                    "scraped_at_utc": cleaned.get("scraped_at_utc"),
                    "winner": cleaned.get("winner"),
                    "loser": cleaned.get("loser"),
                    "margin": cleaned.get("margin"),
                    "total_points": cleaned.get("total_points"),
                    "venue_city": cleaned.get("venue_city"),
                    "venue_state": cleaned.get("venue_state"),
                    "opening_spread_away": cleaned.get("opening_spread_away"),
                    "ats_winner": cleaned.get("ats_winner"),
                    "ou_result": cleaned.get("ou_result"),
                },
            )

