"""
Scraper configuration. If ESPN changes URL patterns or starts requiring
different headers, update this file only.
"""

# ESPN NBA game page base URL. Full URL = BASE_URL + game_id
# Fragility: if ESPN restructures URLs (e.g. /nba/scoreboard/game/...), change here.
BASE_URL = "https://www.espn.com/nba/game/_/gameId/"

# Request headers. A realistic User-Agent reduces chance of being blocked.
# Fragility: if ESPN adds bot detection or requires login, you may need to add
# more headers (e.g. cookies, Referer) here.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# How long to wait for the server to respond (seconds).
REQUEST_TIMEOUT = 10

# Delay between requests (seconds). Randomized in [MIN_DELAY, MAX_DELAY] to be respectful.
MIN_DELAY = 3.0
MAX_DELAY = 6.0

# Number of retries on failure before giving up.
MAX_RETRIES = 3
