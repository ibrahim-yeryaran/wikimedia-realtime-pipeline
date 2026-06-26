-- PostgreSQL container ilk açıldığında bir kez çalışır.
-- Consumer'ın gerçek-zamanlı yazacağı özet (aggregate) tablolarını kurar.

-- 1) Wiki bazında çalışan toplamlar (anlık "en aktif wiki'ler" için)
CREATE TABLE IF NOT EXISTS wiki_totals (
    server_name        VARCHAR(100) PRIMARY KEY,   -- ör. en.wikipedia.org
    total_edits        BIGINT       NOT NULL DEFAULT 0,
    total_bytes_change BIGINT       NOT NULL DEFAULT 0,  -- net byte değişimi
    last_seen_at       TIMESTAMPTZ  NOT NULL
);

-- 2) Dakikalık trafik (zaman serisi: "dakikada kaç düzenleme")
CREATE TABLE IF NOT EXISTS edits_per_minute (
    minute_bucket  TIMESTAMPTZ  NOT NULL,           -- dakikaya yuvarlanmış zaman
    server_name    VARCHAR(100) NOT NULL,
    edit_count     BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_bucket, server_name)
);

-- Zaman-aralığı sorgularını hızlandırmak için
CREATE INDEX IF NOT EXISTS idx_epm_bucket
    ON edits_per_minute (minute_bucket DESC);
