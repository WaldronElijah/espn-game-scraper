-- Add columns for cleaned game records (winner, loser, margin, ATS/OU, venue, etc.).
-- Run after create_table.sql. Existing columns kept for backward compatibility.

ALTER TABLE nba_games
  ADD COLUMN IF NOT EXISTS start_time_utc    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS scraped_at_utc   TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS status           TEXT,
  ADD COLUMN IF NOT EXISTS winner           TEXT,
  ADD COLUMN IF NOT EXISTS loser            TEXT,
  ADD COLUMN IF NOT EXISTS margin           INTEGER,
  ADD COLUMN IF NOT EXISTS total_points     INTEGER,
  ADD COLUMN IF NOT EXISTS venue_name       TEXT,
  ADD COLUMN IF NOT EXISTS venue_city       TEXT,
  ADD COLUMN IF NOT EXISTS venue_state      TEXT,
  ADD COLUMN IF NOT EXISTS opening_spread_home NUMERIC,
  ADD COLUMN IF NOT EXISTS opening_spread_away NUMERIC,
  ADD COLUMN IF NOT EXISTS ats_winner       TEXT,
  ADD COLUMN IF NOT EXISTS ou_result       TEXT;
