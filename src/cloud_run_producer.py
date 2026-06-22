"""
FlightTracker — Cloud Run Producer
Runs as a Cloud Run service that polls OpenSky API
and publishes to Pub/Sub.

This is the "microservicio productor" that the professor mentioned:
[API OpenSky] → [Cloud Run Producer] → [Cloud Pub/Sub] → [BigQuery]

Cloud Run services need to respond to HTTP requests, so we run
the producer in a background thread and expose a /health endpoint.
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

# ──────────────────────────────────────────────
# Configuration (from environment variables)
# ──────────────────────────────────────────────
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "flight-updates")
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "15"))
MAX_DURATION = int(os.environ.get("MAX_DURATION", "600"))  # 10 minutes default

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Flask app (for Cloud Run health checks)
# ──────────────────────────────────────────────
app = Flask(__name__)

# Global stats
stats = {
    "messages_published": 0,
    "total_flights": 0,
    "api_calls": 0,
    "errors": 0,
    "start_time": None,
    "status": "starting"
}


@app.route("/")
def health():
    """Health check endpoint for Cloud Run."""
    return jsonify({
        "status": stats["status"],
        "messages_published": stats["messages_published"],
        "total_flights": stats["total_flights"],
        "api_calls": stats["api_calls"],
        "errors": stats["errors"],
        "uptime_seconds": (time.time() - stats["start_time"]) if stats["start_time"] else 0
    })


@app.route("/health")
def health_check():
    """Alternative health check endpoint."""
    return jsonify({"status": "ok"})


# ──────────────────────────────────────────────
# Producer logic
# ──────────────────────────────────────────────
def fetch_flights():
    """Call OpenSky API and return flight data."""
    try:
        response = requests.get(OPENSKY_API_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"API error: {e}")
        stats["errors"] += 1
        return None


def transform_and_publish(raw_data, publisher, topic_path):
    """Transform API data and publish to Pub/Sub."""
    timestamp = datetime.now(timezone.utc)
    states = raw_data.get("states", [])

    if not states:
        logger.warning("No flight states in response")
        return 0

    flights = []
    for state in states:
        if len(state) < 17:
            continue
        flight = {
            "icao24": state[0],
            "callsign": (state[1] or "").strip(),
            "origin_country": state[2],
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
            "squawk": state[14],
        }
        flights.append(flight)

    message = {
        "metadata": {
            "source": "opensky-api",
            "ingestion_type": "streaming",
            "api_timestamp": raw_data.get("time"),
            "ingestion_timestamp": timestamp.isoformat(),
            "flight_count": len(flights)
        },
        "flights": flights
    }

    message_bytes = json.dumps(message, default=str).encode("utf-8")
    future = publisher.publish(
        topic_path,
        message_bytes,
        source="opensky",
        ingestion_type="streaming",
        flight_count=str(len(flights))
    )
    message_id = future.result()

    stats["messages_published"] += 1
    stats["total_flights"] += len(flights)

    logger.info(f"Published {len(flights)} flights (total: {stats['messages_published']} messages)")
    return len(flights)


def producer_loop():
    """Main producer loop — runs in background thread."""
    logger.info("Starting producer loop...")
    stats["start_time"] = time.time()
    stats["status"] = "running"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)

    while True:
        # Check if we've exceeded max duration
        elapsed = time.time() - stats["start_time"]
        if elapsed > MAX_DURATION:
            logger.info(f"Max duration ({MAX_DURATION}s) reached. Stopping.")
            stats["status"] = "completed"
            break

        # Fetch and publish
        stats["api_calls"] += 1
        raw_data = fetch_flights()

        if raw_data:
            transform_and_publish(raw_data, publisher, topic_path)

        # Wait before next poll
        time.sleep(POLL_INTERVAL)

    logger.info(f"Producer finished. Stats: {stats}")


# ──────────────────────────────────────────────
# Start producer in background thread
# ──────────────────────────────────────────────
producer_thread = threading.Thread(target=producer_loop, daemon=True)
producer_thread.start()


if __name__ == "__main__":
    # Cloud Run sets the PORT environment variable
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
