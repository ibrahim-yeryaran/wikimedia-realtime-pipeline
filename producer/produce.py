"""
produce.py
----------
Connects to Wikimedia's live "recentchange" SSE stream and writes each edit
event to the `wiki.changes` Kafka topic.

Two things are critical in streaming:
  1) Reconnecting automatically when the stream drops
  2) Writing to Kafka efficiently (batched) with delivery acknowledgements
"""

import json
import logging
import os
import signal
import sys

import requests
import sseclient
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("producer")

# ── Settings (from environment, overridden in Docker) ─────────────────────────
STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.changes")

# We only care about real content edits
WANTED_TYPES = {"edit", "new"}


def make_producer() -> Producer:
    """Create the Kafka producer. linger + compression for efficient batched writes."""
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "wiki-producer",
            "linger.ms": 50,            # batch events with a small delay
            "compression.type": "lz4",  # save network/disk
            "acks": "all",              # all in-sync replicas must ack (durability)
        }
    )


def delivery_report(err, msg) -> None:
    """Delivery callback: log on failure."""
    if err is not None:
        log.error("Delivery failed: %s", err)


def stream_events(url: str):
    """
    Read the SSE stream forever; reconnect if the connection drops.
    Yields the raw JSON string (data) of each event.
    """
    headers = {"User-Agent": "wikimedia-realtime-pipeline/1.0 (portfolio project)"}
    backoff = 1
    while True:
        try:
            log.info("Connecting to SSE stream: %s", url)
            resp = requests.get(url, stream=True, headers=headers, timeout=(10, 60))
            resp.raise_for_status()
            client = sseclient.SSEClient(resp)
            backoff = 1  # reset backoff on a successful connection
            for event in client.events():
                if event.data:
                    yield event.data
        except Exception as exc:  # network error, timeout, server closing the stream...
            log.warning("Stream dropped (%s). Reconnecting in %ss.", exc, backoff)
            import time
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)  # exponential backoff (max 30s)


def run() -> None:
    producer = make_producer()

    # On SIGTERM/SIGINT, flush the buffer and exit cleanly
    def shutdown(signum, frame):
        log.info("Shutting down, flushing buffer...")
        producer.flush(10)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info("Producer started → topic=%s, bootstrap=%s", TOPIC, BOOTSTRAP)
    produced = 0
    for data in stream_events(STREAM_URL):
        try:
            evt = json.loads(data)
        except json.JSONDecodeError:
            continue

        if evt.get("type") not in WANTED_TYPES:
            continue

        # key = server_name so events from the same wiki go to the same partition
        key = evt.get("server_name", "unknown")
        producer.produce(
            TOPIC,
            key=key,
            value=json.dumps(evt).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # process delivery callbacks

        produced += 1
        if produced % 100 == 0:
            log.info("%d events produced", produced)


if __name__ == "__main__":
    run()
