-- Reference schema for future SQLite on persistent volume (e.g. Railway /data/npsa.db).
-- Not wired into the app yet — use when you add a queue + durable run index.
--
-- Setup:
--   1. Choose path on volume: e.g. SQLITE_PATH=/data/npsa.sqlite3 (set in Railway).
--   2. On first boot, open DB and execute this file (or run migrations).
--   3. Dual-write: on run start, INSERT/UPDATE runs row AND keep existing JSON metadata.
--   4. Later: switch GET /runs to read from `runs` (JOIN optional paths to CSV).
--
-- Python: sqlite3 is stdlib; use WAL mode for safer concurrent reads:
--   conn.execute("PRAGMA journal_mode=WAL;")

CREATE TABLE IF NOT EXISTS runs (
  run_id            TEXT PRIMARY KEY,
  scraper_type      TEXT NOT NULL CHECK (scraper_type IN ('school', 'church')),
  state             TEXT NOT NULL,
  display_name      TEXT NOT NULL,
  status            TEXT NOT NULL,
  total_counties    INTEGER,
  created_at        TEXT NOT NULL,
  completed_at      TEXT,
  archived          INTEGER NOT NULL DEFAULT 0,
  deleted           INTEGER NOT NULL DEFAULT 0,
  csv_filename      TEXT,
  updated_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_scraper_created
  ON runs (scraper_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_runs_status
  ON runs (status);

-- Optional: job queue (process one at a time per service)
CREATE TABLE IF NOT EXISTS queue_jobs (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  scraper_type      TEXT NOT NULL CHECK (scraper_type IN ('school', 'church')),
  state             TEXT NOT NULL,
  display_name      TEXT,
  status            TEXT NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'running', 'done', 'failed', 'cancelled')),
  run_id            TEXT,
  error             TEXT,
  created_at        TEXT NOT NULL,
  started_at        TEXT,
  finished_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_pending
  ON queue_jobs (status, created_at)
  WHERE status = 'queued';
