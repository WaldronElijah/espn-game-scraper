"""
Clean a single raw game dict from the scraper into a normalized shape.

No file I/O; only transforms one dict. Used by run_cleaner.py to process
data/*.json and optionally by db.insert_cleaned_game for Postgres.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def normalize_status(status: Optional[str]) -> Optional[str]:
    if status is None:
        return None

    status = status.strip().lower()

    mapping = {
        "final": "final",
        "scheduled": "scheduled",
        "in progress": "live",
        "live": "live",
        "halftime": "live",
        "postponed": "postponed",
        "canceled": "canceled",
        "cancelled": "canceled",
    }

    return mapping.get(status, status)


def safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_game_record(raw_game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn one raw scraped game dict into a normalized cleaned dict.

    Raw keys (from parse_game): game_id, date, game_status, away_team, home_team,
    away_score, home_score, location, referees, opening_spread, opening_total,
    draftkings_lines.

    Cleaned adds: start_time_utc, scraped_at_utc, status, winner, loser, margin,
    total_points, venue_name, venue_city, venue_state, opening_spread_home,
    opening_spread_away, ats_winner, ou_result.
    """
    game_id = str(raw_game.get("game_id")) if raw_game.get("game_id") is not None else None

    start_time_utc = raw_game.get("date")
    status = normalize_status(raw_game.get("game_status"))

    away_team = raw_game.get("away_team")
    home_team = raw_game.get("home_team")

    away_score = safe_int(raw_game.get("away_score"))
    home_score = safe_int(raw_game.get("home_score"))

    referees = raw_game.get("referees")
    if not isinstance(referees, list):
        referees = []

    opening_spread_home = safe_float(raw_game.get("opening_spread"))
    opening_spread_away = None
    if opening_spread_home is not None:
        opening_spread_away = round(opening_spread_home * -1, 1)

    opening_total = safe_float(raw_game.get("opening_total"))

    winner = None
    loser = None
    margin = None
    total_points = None
    ats_winner = None
    ou_result = None

    if away_score is not None and home_score is not None:
        total_points = away_score + home_score
        margin = abs(away_score - home_score)

        if away_score > home_score:
            winner = away_team
            loser = home_team
        elif home_score > away_score:
            winner = home_team
            loser = away_team

        # ATS: opening_spread is from home team perspective (e.g. -4.5 = home favored by 4.5).
        if opening_spread_home is not None:
            adjusted_home_score = home_score + opening_spread_home

            if adjusted_home_score > away_score:
                ats_winner = home_team
            elif adjusted_home_score < away_score:
                ats_winner = away_team
            else:
                ats_winner = "push"

        if opening_total is not None:
            if total_points > opening_total:
                ou_result = "over"
            elif total_points < opening_total:
                ou_result = "under"
            else:
                ou_result = "push"

    cleaned = {
        "game_id": game_id,
        "start_time_utc": start_time_utc,
        "scraped_at_utc": iso_utc_now(),
        "status": status,
        "away_team": away_team,
        "home_team": home_team,
        "away_score": away_score,
        "home_score": home_score,
        "winner": winner,
        "loser": loser,
        "margin": margin,
        "total_points": total_points,
        "venue_name": raw_game.get("location"),
        "venue_city": None,
        "venue_state": None,
        "referees": referees,
        "opening_spread_home": opening_spread_home,
        "opening_spread_away": opening_spread_away,
        "opening_total": opening_total,
        "ats_winner": ats_winner,
        "ou_result": ou_result,
        "draftkings_lines": raw_game.get("draftkings_lines"),
    }

    return cleaned
