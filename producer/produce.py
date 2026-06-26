"""
produce.py
----------
Wikimedia'nın canlı "recentchange" SSE akışına bağlanır ve her düzenleme
olayını Kafka'daki `wiki.changes` topic'ine yazar.

Streaming'de iki şey kritik:
  1) Akış koparsa otomatik yeniden bağlanmak
  2) Kafka'ya verimli (batch'li) ve teslimat-onaylı yazmak
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

# ── Ayarlar (ortam değişkeninden, Docker'da override edilir) ──────────────────
STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.changes")

# Sadece gerçek içerik düzenlemeleriyle ilgileniyoruz
WANTED_TYPES = {"edit", "new"}


def make_producer() -> Producer:
    """Kafka producer'ı oluşturur. linger + compression ile verimli batch yazımı."""
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "wiki-producer",
            "linger.ms": 50,            # küçük gecikmeyle olayları batch'le
            "compression.type": "lz4",  # ağ/disk tasarrufu
            "acks": "all",              # tüm in-sync replikalar onaylasın (güvenilirlik)
        }
    )


def delivery_report(err, msg) -> None:
    """Teslimat geri-çağrısı: hata olursa logla."""
    if err is not None:
        log.error("Teslimat başarısız: %s", err)


def stream_events(url: str):
    """
    SSE akışını sonsuza dek okur; bağlantı koparsa yeniden bağlanır.
    Her olayın ham JSON string'ini (data) üretir (yield).
    """
    headers = {"User-Agent": "wikimedia-realtime-pipeline/1.0 (portfolio project)"}
    backoff = 1
    while True:
        try:
            log.info("SSE akışına bağlanılıyor: %s", url)
            resp = requests.get(url, stream=True, headers=headers, timeout=(10, 60))
            resp.raise_for_status()
            client = sseclient.SSEClient(resp)
            backoff = 1  # başarılı bağlantıda backoff sıfırlanır
            for event in client.events():
                if event.data:
                    yield event.data
        except Exception as exc:  # ağ hatası, timeout, sunucu kapatması...
            log.warning("Akış koptu (%s). %ss sonra yeniden bağlanılıyor.", exc, backoff)
            import time
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)  # üstel geri çekilme (max 30s)


def run() -> None:
    producer = make_producer()

    # SIGTERM/SIGINT'te tamponu boşaltıp temiz çık
    def shutdown(signum, frame):
        log.info("Kapatılıyor, tampon boşaltılıyor...")
        producer.flush(10)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info("Producer başladı → topic=%s, bootstrap=%s", TOPIC, BOOTSTRAP)
    produced = 0
    for data in stream_events(STREAM_URL):
        try:
            evt = json.loads(data)
        except json.JSONDecodeError:
            continue

        if evt.get("type") not in WANTED_TYPES:
            continue

        # Aynı wiki'nin olayları aynı partition'a gitsin diye key = server_name
        key = evt.get("server_name", "unknown")
        producer.produce(
            TOPIC,
            key=key,
            value=json.dumps(evt).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # teslimat geri-çağrılarını işle

        produced += 1
        if produced % 100 == 0:
            log.info("%d olay üretildi", produced)


if __name__ == "__main__":
    run()
