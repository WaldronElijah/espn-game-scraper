"""
Parse a single ESPN NBA game summary JSON into a clean Python dictionary.

We no longer scrape HTML or <script> tags. Instead, we call the official
ESPN summary endpoint (see fetch_game.fetch_summary) and walk that JSON.

Main output fields:
- game_id
- date
- home_team / away_team
- home_score / away_score
- game_status (e.g. "Final")
- location (arena + city if available)
- referees (list of names, if available)
- opening_spread
- opening_total
- draftkings_lines (raw DraftKings odds dict, if available)
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional


def _safe_get(obj: Any, path: List[Any], default: Any = None) -> Any:
    """
    Walk a nested dict/list structure safely.

    Example:
        _safe_get(data, ["gamepackageJSON", "header", "competitions", 0, "date"])
    """
    cur: Any = obj
    for key in path:
        if isinstance(key, int):
            if not isinstance(cur, list) or key >= len(cur):
                return default
            cur = cur[key]
        else:
            if not isinstance(cur, Mapping) or key not in cur:  # type: ignore[arg-type]
                return default
            cur = cur[key]  # type: ignore[index]
    return cur


def _extract_teams_and_scores(summary: Dict[str, Any]) -> Dict[str, Optional[Any]]:
    """
    Extract home/away team names and scores from the game JSON.

    We don't assume a fixed order; instead we look at the 'homeAway' field
    on each competitor.
    """
    competitions0 = _safe_get(
        summary, ["header", "competitions", 0], default={}
    ) or {}
    competitors = competitions0.get("competitors") or []

    home_team: Optional[str] = None
    away_team: Optional[str] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None

    for comp in competitors:
        if not isinstance(comp, Mapping):
            continue
        side = comp.get("homeAway")
        team_name = _safe_get(comp, ["team", "displayName"])
        score_raw = comp.get("score")
        try:
            score_val: Optional[int] = int(score_raw) if score_raw is not None else None
        except (TypeError, ValueError):
            score_val = None

        if side == "home":
            home_team = team_name
            home_score = score_val
        elif side == "away":
            away_team = team_name
            away_score = score_val

    return {
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
    }


def _extract_status_and_date(summary: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Extract game status (e.g. 'Final') and scheduled date/time.
    """
    competitions0 = _safe_get(
        summary, ["header", "competitions", 0], default={}
    ) or {}

    status = _safe_get(
        competitions0,
        ["status", "type", "description"],
        default=None,
    )

    # The date is usually on the competition itself.
    date = competitions0.get("date") or _safe_get(
        summary,
        ["header", "competitions", 0, "date"],
        default=None,
    )

    return {"game_status": status, "date": date}


def _extract_location(summary: Dict[str, Any]) -> Optional[str]:
    """
    Build a human-readable location string from venue information.

    Example output:
        "Crypto.com Arena, Los Angeles, CA"
    """
    competitions0 = _safe_get(
        summary, ["header", "competitions", 0], default={}
    ) or {}

    venue = competitions0.get("venue") or {}
    name = venue.get("fullName")
    city = _safe_get(venue, ["address", "city"])
    state = _safe_get(venue, ["address", "state"])

    parts = []
    if name:
        parts.append(str(name))

    city_state_parts = [p for p in (city, state) if p]
    if city_state_parts:
        parts.append(", ".join(str(p) for p in city_state_parts))

    if not parts:
        return None
    return ", ".join(parts)


def _extract_referees(summary: Dict[str, Any]) -> List[str]:
    """
    Extract referee names, if present.

    ESPN may list officials either under the competition or under gameInfo.
    """
    competitions0 = _safe_get(
        summary, ["header", "competitions", 0], default={}
    ) or {}

    officials = (
        competitions0.get("officials")
        or _safe_get(summary, ["gameInfo", "officials"], default=[])
        or []
    )

    names: List[str] = []
    for off in officials:
        if not isinstance(off, Mapping):
            continue
        name = off.get("fullName") or off.get("displayName")
        if name:
            names.append(str(name))

    return names


def _extract_betting(summary: Dict[str, Any]) -> Dict[str, Optional[Any]]:
    """
    Extract opening spread/total and DraftKings-specific lines.

    Fragility:
    - The 'pickcenter' structure and provider names are not guaranteed.
    - We use .get() everywhere so missing data just returns None.
    """
    pickcenter = summary.get("pickcenter") or []

    opening_spread: Optional[float] = None
    opening_total: Optional[float] = None
    draftkings_lines: Optional[Dict[str, Any]] = None

    for entry in pickcenter:
        if not isinstance(entry, Mapping):
            continue

        # First available line becomes the "opening" line.
        if opening_spread is None and "spread" in entry:
            try:
                opening_spread = float(entry["spread"])
            except (TypeError, ValueError):
                opening_spread = None

        if opening_total is None and "overUnder" in entry:
            try:
                opening_total = float(entry["overUnder"])
            except (TypeError, ValueError):
                opening_total = None

        provider_name = _safe_get(entry, ["provider", "name"], default="")
        if (
            provider_name
            and isinstance(provider_name, str)
            and provider_name.lower() == "draftkings"
            and draftkings_lines is None
        ):
            # Keep the full entry so you have all DK markets available later.
            draftkings_lines = dict(entry)

    return {
        "opening_spread": opening_spread,
        "opening_total": opening_total,
        "draftkings_lines": draftkings_lines,
    }


def parse_game(summary: Dict[str, Any], game_id: str) -> Dict[str, Any]:
    """
    Parse one ESPN NBA game page into a clean dictionary.

    This function does not make any network calls. It only:
    - walks the already-fetched ESPN summary JSON defensively
    - returns a flat dict you can save as JSON or insert into PostgreSQL
    """
    teams_scores = _extract_teams_and_scores(summary)
    status_date = _extract_status_and_date(summary)
    location = _extract_location(summary)
    referees = _extract_referees(summary)
    betting = _extract_betting(summary)

    result: Dict[str, Any] = {
        "game_id": game_id,
        "date": status_date["date"],
        "away_team": teams_scores["away_team"],
        "home_team": teams_scores["home_team"],
        "away_score": teams_scores["away_score"],
        "home_score": teams_scores["home_score"],
        "game_status": status_date["game_status"],
        "location": location,
        "referees": referees,
        "opening_spread": betting["opening_spread"],
        "opening_total": betting["opening_total"],
        "draftkings_lines": betting["draftkings_lines"],
    }

    return result

