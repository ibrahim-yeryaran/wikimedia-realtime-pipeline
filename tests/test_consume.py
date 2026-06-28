"""
Unit tests for the consumer's parse_event — no Kafka or PostgreSQL needed.
"""

import sys
from pathlib import Path

# Make the consumer module importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "consumer"))

import consume  # noqa: E402


def test_parse_event_valid():
    raw = b'{"server_name":"en.wikipedia.org","timestamp":1719400000,"type":"edit","length":{"old":1200,"new":1500}}'
    record = consume.parse_event(raw)
    assert record["server"] == "en.wikipedia.org"
    assert record["bytes"] == 300            # 1500 - 1200
    assert record["bucket"].second == 0      # truncated to the minute


def test_parse_event_invalid_json():
    assert consume.parse_event(b"not-json") is None


def test_parse_event_missing_fields():
    # Has a timestamp but no server_name → invalid
    assert consume.parse_event(b'{"timestamp":1719400000}') is None
