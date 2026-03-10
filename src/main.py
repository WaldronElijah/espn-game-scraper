
from __future__ import annotations

import json
import logging
import pathlib
import random
import time
from typing import Iterable

from src.config import MAX_DELAY, MIN_DELAY
from src.fetch_game import fetch_summary
from src.parse_game import parse_game


DATA_DIR = pathlib.Path("data")

# Default: skip if we already have a file updated in the last 24 hours.
# Set to None to skip whenever the file exists (no age check).
DEFAULT_MAX_AGE_SECONDS = 86400  # 24 hours


def already_scraped(game_id: str, max_age_seconds: float | None = DEFAULT_MAX_AGE_SECONDS) -> bool:
    """
    Return True if we should skip this game (already have fresh data).

    - If max_age_seconds is None: skip whenever data/<game_id>.json exists.
    - Otherwise: skip only if the file exists and was modified within the last
      max_age_seconds (e.g. 86400 = 24 hours). Older files will be rescraped.
    """
    path = DATA_DIR / f"{game_id}.json"
    if not path.is_file():
        return False
    if max_age_seconds is None:
        return True
    age = time.time() - path.stat().st_mtime
    return age <= max_age_seconds


def save_json(game_data: dict) -> pathlib.Path:
    """
    Save one game's data as a JSON file under the data/ folder.

    The filename is <game_id>.json so it's easy to look up later.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    path = DATA_DIR / f"{game_data['game_id']}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(game_data, f, indent=2, ensure_ascii=False)

    return path


def scrape_single_game(
    game_id: str,
    max_age_seconds: float | None = DEFAULT_MAX_AGE_SECONDS,
) -> dict | None:
    """
    High-level helper for ONE game:
    1. If we already have a fresh JSON file, skip (return None).
    2. Otherwise fetch JSON summary from ESPN, parse, save to data/<game_id>.json.
    """
    if already_scraped(game_id, max_age_seconds):
        return None
    summary = fetch_summary(game_id)
    game_data = parse_game(summary, game_id)
    save_json(game_data)
    return game_data


def scrape_games(
    game_ids: Iterable[str],
    max_age_seconds: float | None = DEFAULT_MAX_AGE_SECONDS,
    min_delay: float | None = None,
    max_delay: float | None = None,
) -> None:
    """
    Scrape multiple games in sequence with logging and respectful delays.

    If a game was already scraped recently (see already_scraped), we skip it
    and do not call ESPN. Only when we actually fetch do we sleep between requests.

    min_delay/max_delay override config when provided (e.g. for a slow range run).
    """
    delay_lo = min_delay if min_delay is not None else MIN_DELAY
    delay_hi = max_delay if max_delay is not None else MAX_DELAY

    for game_id in game_ids:
        try:
            logging.info("Scraping game %s", game_id)
            data = scrape_single_game(game_id, max_age_seconds)
            if data is None:
                logging.info("Skipping game %s (already scraped, file is fresh)", game_id)
            else:
                logging.info(
                    "Scraped game %s: %s @ %s (%s-%s) status=%s",
                    game_id,
                    data.get("away_team"),
                    data.get("home_team"),
                    data.get("away_score"),
                    data.get("home_score"),
                    data.get("game_status"),
                )
                # Respectful pacing: only sleep after we actually hit ESPN.
                delay = random.uniform(delay_lo, delay_hi)
                logging.info("Sleeping %.1f seconds before next game", delay)
                time.sleep(delay)
        except Exception as exc:
            logging.exception("Failed to scrape game %s: %s", game_id, exc)
            # Sleep after an error too, to avoid hammering on repeated failures.
            delay = random.uniform(delay_lo, delay_hi)
            time.sleep(delay)


def _configure_logging() -> None:
    """
    Configure basic console logging with timestamps.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


if __name__ == "__main__":
    _configure_logging()

    START_GAME_ID = 401810647
    END_GAME_ID = 401810783
    game_ids = [str(i) for i in range(START_GAME_ID, END_GAME_ID + 1)]

    logging.info(
        "Scraping %d games from ID %s to %s (slow delays)",
        len(game_ids),
        START_GAME_ID,
        END_GAME_ID,
    )
    scrape_games(
        game_ids,
        min_delay=6.0,
        max_delay=12.0,
    )

