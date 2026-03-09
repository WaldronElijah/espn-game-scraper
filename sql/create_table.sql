-- Flat table for scraped ESPN NBA game data.
-- This is intentionally simple and denormalized for ease of use.

CREATE TABLE IF NOT EXISTS nba_games (
    game_id          TEXT PRIMARY KEY,
    "date"           TIMESTAMPTZ,
    away_team        TEXT,
    home_team        TEXT,
    away_score       INTEGER,
    home_score       INTEGER,
    game_status      TEXT,
    location         TEXT,
    referees         TEXT[],   -- PostgreSQL text array (e.g. '{"Ref 1","Ref 2"}')
    opening_spread   NUMERIC,
    opening_total    NUMERIC,
    draftkings_lines JSONB,    -- Flexible container for all DraftKings markets
    scraped_at       TIMESTAMPTZ DEFAULT NOW()
);

