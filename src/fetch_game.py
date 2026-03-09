"""
HTTP helpers for talking to ESPN.

Originally this module fetched full HTML pages and we tried to scrape a
JSON blob out of a <script> tag. ESPN has since moved key data into a
separate JSON summary endpoint, which is more stable and simpler to use.

We now call that summary endpoint directly.
"""

import time
from typing import Any, Dict

import requests

from src.config import (
    HEADERS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)

# ESPN NBA game summary endpoint.
# Fragility: if ESPN changes this URL or its query parameters, update here.
SUMMARY_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"


def fetch_summary(game_id: str) -> Dict[str, Any]:
    """
    Fetch the official ESPN JSON summary for a single NBA game.

    This is more reliable than scraping HTML because ESPN themselves use
    this endpoint to power their game pages.

    Args:
        game_id: ESPN game ID (e.g. "401810777").

    Returns:
        Parsed JSON as a Python dict.

    Raises:
        requests.RequestException: On HTTP errors or after all retries fail.
        ValueError: If the JSON structure looks wrong (missing 'header').
    """
    params = {
        "region": "us",
        "lang": "en",
        "contentorigin": "espn",
        "event": game_id,
    }

    last_error: Exception | None = None

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                SUMMARY_URL,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()

            # Basic sanity check: real summaries always have a header.competitions.
            if "header" not in data:
                raise ValueError("Summary JSON missing 'header' key")

            return data

        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == MAX_RETRIES - 1:
                break
            # Exponential backoff: 1s, 2s, 4s, ...
            sleep_secs = 2**attempt
            time.sleep(sleep_secs)

    # If we get here, all retries failed.
    if last_error:
        raise last_error
    raise RuntimeError("Unexpected: retries exhausted without capturing an error")


# The old HTML-based fetch function is kept here for debugging/experiments.
# It is no longer used by the main scraper pipeline.
def fetch_page(game_id: str) -> str:
    """
    Legacy helper: fetch the raw HTML page for a game.

    This is not used by the main pipeline anymore, but it can be handy
    when you want to manually inspect the HTML in a browser or debugger.
    """
    from src.config import BASE_URL  # imported lazily to avoid unused warning

    url = BASE_URL + game_id
    response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text
