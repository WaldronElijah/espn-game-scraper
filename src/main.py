
from __future__ import annotations

import json
import logging
import pathlib
import random
import time
from typing import Iterable, List

from src.config import MAX_DELAY, MIN_DELAY
from src.fetch_game import fetch_page
from src.parse_game import parse_game


DATA_DIR = pathlib.Path("data")


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


def scrape_single_game(game_id: str) -> dict:
    """
    High-level helper for ONE game:
    1. Fetch HTML from ESPN.
    2. Parse it into a clean dict.
    3. Save it to data/<game_id>.json.
    """
    html = fetch_page(game_id)
    game_data = parse_game(html, game_id)
    save_json(game_data)
    return game_data


def scrape_games(game_ids: Iterable[str]) -> None:
    """
    Scrape multiple games in sequence with logging and respectful delays.

    This is how you scale from:
        "one game" → "one week" → "one month"

    You control how many games by changing the game_ids list.
    """
    for game_id in game_ids:
        try:
            logging.info("Scraping game %s", game_id)
            data = scrape_single_game(game_id)
            logging.info(
                "Scraped game %s: %s @ %s (%s-%s) status=%s",
                game_id,
                data.get("away_team"),
                data.get("home_team"),
                data.get("away_score"),
                data.get("home_score"),
                data.get("game_status"),
            )
        except Exception as exc:
            # We log the exception and continue instead of crashing the whole run.
            logging.exception("Failed to scrape game %s: %s", game_id, exc)
        finally:
            # Respectful pacing: sleep a random amount between games.
            # This helps avoid hammering ESPN's servers in a predictable pattern.
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            logging.info("Sleeping %.1f seconds before next game", delay)
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

    # Start extremely small: ONE game.
    # You can change this list to:
    #   - a week of game_ids
    #   - a month of game_ids
    # as long as you keep the delays respectful.
    #
    # Example IDs :
    #   401810777, 401810770
    game_ids: List[str] = [
        "401810777",
        # "401810770",
    ]

    scrape_games(game_ids)

