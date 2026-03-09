"""
Fetch a single ESPN NBA game page over HTTP.
Uses retries, timeout, and backoff to be respectful and resilient.
"""

import time

import requests

from src.config import (
    BASE_URL,
    HEADERS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)

# Minimum expected HTML length. ESPN game pages are large; a tiny response
# often means an error page or CAPTCHA. Fragility: if ESPN slims the page,
# lower this or remove the check.
MIN_CONTENT_LENGTH = 50000


def fetch_page(game_id: str) -> str:
    """
    Fetch the raw HTML for one ESPN NBA game page.

    Args:
        game_id: ESPN game ID (e.g. "401810777").

    Returns:
        Raw HTML string.

    Raises:
        requests.RequestException: On HTTP errors or after all retries fail.
        ValueError: If response body is suspiciously small (e.g. error/CAPTCHA page).
    """
    url = BASE_URL + game_id

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            # Raises for 4xx/5xx status codes.
            response.raise_for_status()

            html = response.text
            # ESPN can return 200 with a thin error/CAPTCHA page.
            if len(html) < MIN_CONTENT_LENGTH:
                raise ValueError(
                    f"Response too short ({len(html)} chars); possible error or CAPTCHA page"
                )
            return html

        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            # Exponential backoff: 1s, 2s, 4s, ...
            sleep_secs = 2**attempt
            time.sleep(sleep_secs)

    # Should not reach here; raise_for_status or ValueError exits above.
    raise RuntimeError("Unexpected: retries exhausted without return or raise")
