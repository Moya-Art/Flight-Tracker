"""
FlightTracker — Cloud Run Producer (Continuous, Dataflow-compatible)
Polls OpenSky API and publishes ONE message per flight to Pub/Sub.

Architecture:
    [OpenSky API] → [Cloud Run] → [Pub/Sub] → [Dataflow] → [BigQuery]
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify
from google.cloud import pubsub_v1

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "flight-updates")
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "25"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

stats = {
    "messages_published": 0,
    "api_calls": 0,
    "errors": 0,
    "start_time": None,
    "status": "starting",
    "last_publish": None
}


@app.route("/")
def health():
    uptime = (time.time() - stats["start_time"]) if stats["start_time"] else 0
    return jsonify({
        "status": stats["status"],
        "service": "FlightTracker Producer",
        "messages_published": stats["messages_published"],
        "api_calls": stats["api_calls"],
        "errors": stats["errors"],
        "uptime_seconds": round(uptime),
        "uptime_human": f"{int(uptime//3600)}h {int((uptime%3600)//60)}m {int(uptime%60)}s",
        "last_publish": stats["last_publish"],
        "poll_interval_seconds": POLL_INTERVAL
    })


@app.route("/health")
def health_check():
    return jsonify({"status": "ok"})


def fetch_flights():
    try:
        response = requests.get(OPENSKY_API_URL, timeout=30)
        response.raise_for_status()
        stats["api_calls"] += 1
        return response.json()
    except Exception as e:
        logger.warning(f"API error: {e}")
        stats["errors"] += 1
        return None


def transform_and_publish(raw_data, publisher, topic_path):
    timestamp = datetime.now(timezone.utc)
    states = raw_data.get("states", [])
    if not states:
        return 0

    published = 0
    for state in states:
        if len(state) < 17:
            continue

        flight = {
            "icao24": state[0] or "",
            "callsign": (state[1] or "").strip(),
            "origin_country": state[2] or "",
            "time_position": state[3],
            "last_contact": state[4],
            "longitude": state[5],
            "latitude": state[6],
            "baro_altitude": state[7],
            "on_ground": state[8],
            "velocity": state[9],
            "true_track": state[10],
            "vertical_rate": state[11],
            "geo_altitude": state[13],
            "squawk": state[14] or "",
            "ingestion_source": "streaming",
            "ingestion_timestamp": timestamp.isoformat()
        }

        message_bytes = json.dumps(flight, default=str).encode("utf-8")
        publisher.publish(topic_path, message_bytes)
        published += 1

    stats["messages_published"] += published
    stats["last_publish"] = timestamp.isoformat()
    logger.info(f"Published {published} flights (total: {stats['messages_published']})")
    return published


def producer_loop():
    logger.info("FlightTracker Producer — Continuous Mode")
    stats["start_time"] = time.time()
    stats["status"] = "running"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)
    consecutive_errors = 0

    while True:
        try:
            if consecutive_errors >= 10:
                time.sleep(60)
                consecutive_errors = 0

            raw_data = fetch_flights()
            if raw_data:
                n = transform_and_publish(raw_data, publisher, topic_path)
                if n > 0:
                    consecutive_errors = 0
            else:
                consecutive_errors += 1

            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.error(f"Error: {e}")
            consecutive_errors += 1
            time.sleep(POLL_INTERVAL)


threading.Thread(target=producer_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
