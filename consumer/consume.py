"""
consume.py
----------
Consumes the `wiki.changes` Kafka topic and writes each event into the
summary tables in PostgreSQL (running counters via upserts).

Delivery semantics: **at-least-once**.
  - We write + commit to the DB FIRST, then commit the Kafka offset.
  - So on a crash, at worst some events are reprocessed (never lost). Since the
    upserts increment totals, we note this in the README.
"""

# Makes type hints lazy → `dict | None` also works on older Python versions
from __future__ import annotations

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("consumer")

# ── Settings ──────────────────────────────────────────────────────────────────
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.changes")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wiki-consumer")

DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "wikistream"),
    "user": os.getenv("DB_USER", "stream"),
    "password": os.getenv("DB_PASSWORD", "stream"),
}

# How many events between each DB commit + offset commit
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

UPSERT_TOTALS = """
    INSERT INTO wiki_totals (server_name, total_edits, total_bytes_change, last_seen_at)
    VALUES (%(server)s, 1, %(bytes)s, %(ts)s)
    ON CONFLICT (server_name) DO UPDATE SET
        total_edits        = wiki_totals.total_edits + 1,
        total_bytes_change = wiki_totals.total_bytes_change + EXCLUDED.total_bytes_change,
        last_seen_at       = EXCLUDED.last_seen_at;
"""

UPSERT_PER_MINUTE = """
    INSERT INTO edits_per_minute (minute_bucket, server_name, edit_count)
    VALUES (%(bucket)s, %(server)s, 1)
    ON CONFLICT (minute_bucket, server_name) DO UPDATE SET
        edit_count = edits_per_minute.edit_count + 1;
"""


def parse_event(raw: bytes) -> dict | None:
    """Turn a Kafka message into a processable summary; None if invalid."""
    try:
        evt = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

    server = evt.get("server_name")
    ts = evt.get("timestamp")
    if not server or ts is None:
        return None

    when = datetime.fromtimestamp(ts, tz=timezone.utc)
    length = evt.get("length") or {}
    bytes_change = (length.get("new") or 0) - (length.get("old") or 0)

    return {
        "server": server,
        "ts": when,
        "bucket": when.replace(second=0, microsecond=0),  # truncate to the minute
        "bytes": bytes_change,
    }


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",   # on first run, start from the newest
            "enable.auto.commit": False,     # we commit offsets manually
        }
    )
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(**DB_CONF)
    log.info("Consumer started → topic=%s, group=%s", TOPIC, GROUP_ID)

    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    pending = 0
    processed = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                continue

            record = parse_event(msg.value())
            if record is None:
                continue

            with conn.cursor() as cur:
                cur.execute(UPSERT_TOTALS, record)
                cur.execute(UPSERT_PER_MINUTE, record)

            pending += 1
            processed += 1

            # When the batch is full: commit the DB first, then the Kafka offset (at-least-once)
            if pending >= BATCH_SIZE:
                conn.commit()
                consumer.commit(asynchronous=False)
                pending = 0
                log.info("%d events processed (written to DB)", processed)
    finally:
        log.info("Shutting down, committing remaining %d events...", pending)
        conn.commit()
        consumer.commit(asynchronous=False)
        conn.close()
        consumer.close()
        sys.exit(0)


if __name__ == "__main__":
    run()
