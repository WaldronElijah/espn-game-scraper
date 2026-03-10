"""
Load raw game JSON from data/, clean each, write to data/cleaned/.

Run: python -m src.run_cleaner
Optional: python -m src.run_cleaner --db  (also insert cleaned records into Postgres)
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib

from src.clean import clean_game_record


# Same convention as main.py: raw scraper output lives in data/
DATA_DIR = pathlib.Path("data")
CLEAN_DIR = DATA_DIR / "cleaned"


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def run_cleaner(insert_into_db: bool = False) -> None:
    """
    Read each raw JSON file in data/ (one game per file), clean, write to data/cleaned/.
    If insert_into_db is True, also call insert_cleaned_game for each cleaned record.
    """
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # Only top-level .json files in data/ (exclude data/cleaned/*.json)
    raw_files = sorted(f for f in DATA_DIR.iterdir() if f.is_file() and f.suffix == ".json")

    if not raw_files:
        logging.warning("No .json files found in %s", DATA_DIR)
        return

    logging.info("Cleaning %d files from %s -> %s", len(raw_files), DATA_DIR, CLEAN_DIR)

    for path in raw_files:
        try:
            with path.open("r", encoding="utf-8") as f:
                raw_data = json.load(f)

            if isinstance(raw_data, list):
                cleaned_list = [clean_game_record(g) for g in raw_data]
                # One file with many games: write one file per game or one combined file.
                # Plan said one game per file; if we get a list, write one output file per game.
                for i, cleaned in enumerate(cleaned_list):
                    game_id = cleaned.get("game_id") or str(i)
                    out_path = CLEAN_DIR / f"{game_id}.json"
                    with out_path.open("w", encoding="utf-8") as out:
                        json.dump(cleaned, out, indent=2, ensure_ascii=False)
                    if insert_into_db:
                        _insert_cleaned(cleaned)
                logging.info("Cleaned %s (%d games)", path.name, len(cleaned_list))
            else:
                cleaned = clean_game_record(raw_data)
                out_path = CLEAN_DIR / path.name
                with out_path.open("w", encoding="utf-8") as out:
                    json.dump(cleaned, out, indent=2, ensure_ascii=False)
                if insert_into_db:
                    _insert_cleaned(cleaned)
                logging.info("Cleaned %s", path.name)

        except Exception as e:
            logging.exception("Failed %s: %s", path.name, e)


def _insert_cleaned(cleaned: dict) -> None:
    """Call db.insert_cleaned_game if available; log and skip on error."""
    try:
        from src.db import insert_cleaned_game
        insert_cleaned_game(cleaned)
    except Exception as e:
        logging.exception("DB insert failed for game_id=%s: %s", cleaned.get("game_id"), e)


if __name__ == "__main__":
    _configure_logging()

    parser = argparse.ArgumentParser(description="Clean raw game JSON and optionally insert into Postgres")
    parser.add_argument(
        "--db",
        action="store_true",
        help="After cleaning, insert each record into Postgres",
    )
    args = parser.parse_args()

    run_cleaner(insert_into_db=args.db)
