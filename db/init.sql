-- Runs once when the PostgreSQL container is first created.
-- Sets up the summary (aggregate) tables the consumer writes to in real time.

-- 1) Running totals per wiki (for an instant "most active wikis" view)
CREATE TABLE IF NOT EXISTS wiki_totals (
    server_name        VARCHAR(100) PRIMARY KEY,   -- e.g. en.wikipedia.org
    total_edits        BIGINT       NOT NULL DEFAULT 0,
    total_bytes_change BIGINT       NOT NULL DEFAULT 0,  -- net byte change
    last_seen_at       TIMESTAMPTZ  NOT NULL
);

-- 2) Per-minute traffic (time series: "edits per minute")
CREATE TABLE IF NOT EXISTS edits_per_minute (
    minute_bucket  TIMESTAMPTZ  NOT NULL,           -- time truncated to the minute
    server_name    VARCHAR(100) NOT NULL,
    edit_count     BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_bucket, server_name)
);

-- Speeds up time-range queries
CREATE INDEX IF NOT EXISTS idx_epm_bucket
    ON edits_per_minute (minute_bucket DESC);
