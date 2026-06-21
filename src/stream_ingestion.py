"""
FlightTracker — Stream Ingestion (Producer)
Polls the OpenSky Network API every 15 seconds and publishes
each batch of flight data to Google Cloud Pub/Sub.

This is the STREAMING/REAL-TIME layer of the pipeline.
Run this script continuously to simulate real-time flight tracking.

Architecture:
    OpenSky API  →  [This Script]  →  Pub/Sub Topic  →  Subscriber  →  BigQuery
"""
import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1

sys.path.insert(0, ".")
from config.settings import (
    GCP_PROJECT_ID, PUBSUB_TOPIC, 
    OPENSKY_API_URL, OPENSKY_POLL_INTERVAL
)

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/stream_ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FlightStreamProducer:
    """
    Polls OpenSky API and publishes flight data to Pub/Sub.
    
    Why Pub/Sub instead of writing directly to BigQuery?
    1. Decoupling: The producer doesn't need to know about BigQuery
    2. Buffering: If BigQuery is slow, messages queue up instead of being lost
    3. Multiple consumers: Other services could also read from this topic
    4. Resilience: Pub/Sub guarantees at-least-once delivery
    """
    
    def __init__(self):
        self.project_id = GCP_PROJECT_ID
        self.topic_id = PUBSUB_TOPIC
        self.api_url = OPENSKY_API_URL
        self.poll_interval = OPENSKY_POLL_INTERVAL
        
        # Create the Pub/Sub publisher client
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        
        # Track statistics
        self.total_messages_published = 0
        self.total_api_calls = 0
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5  # Stop after 5 failures in a row
    
    def fetch_flights(self):
        """
        Call the OpenSky API and return the raw response.
        
        Error handling:
        - Timeout: API might be slow, wait and retry
        - Connection error: Network issue, log and continue
        - HTTP error: API returned an error, log the status code
        """
        try:
            response = requests.get(self.api_url, timeout=30)
            response.raise_for_status()
            self.consecutive_errors = 0  # Reset error counter on success
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.warning("API request timed out — will retry next cycle")
            self.consecutive_errors += 1
            return None
        except requests.exceptions.ConnectionError:
            logger.warning("Connection error — OpenSky API may be down")
            self.consecutive_errors += 1
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error: {e}")
            self.consecutive_errors += 1
            return None
    
    def transform_and_publish(self, raw_data):
        """
        Transform raw API data and publish each flight as a message to Pub/Sub.
        
        We publish one message per BATCH of flights (not one per flight).
        This is more efficient — fewer API calls to Pub/Sub, lower cost.
        
        Each message is a JSON object with:
        - metadata (timestamp, count, source)
        - flights (list of flight records)
        """
        timestamp = datetime.now(timezone.utc)
        states = raw_data.get("states", [])
        
        if not states:
            logger.warning("No flight states in API response")
            return 0
        
        # Transform positional arrays into named dictionaries
        flights = []
        for state in states:
            if len(state) < 17:
                continue  # Skip malformed records
            
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
        
        # Create the message payload
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
        
        # Publish to Pub/Sub
        # The message is serialized as JSON bytes
        message_bytes = json.dumps(message, default=str).encode("utf-8")
        
        future = self.publisher.publish(
            self.topic_path,
            message_bytes,
            # Attributes help with filtering in Pub/Sub
            source="opensky",
            ingestion_type="streaming",
            flight_count=str(len(flights))
        )
        
        # Wait for the publish to complete
        message_id = future.result()
        self.total_messages_published += 1
        
        logger.info(
            f"Published message {message_id} with {len(flights)} flights "
            f"(total published: {self.total_messages_published})"
        )
        
        return len(flights)
    
    def run(self):
        """
        Main loop: poll API → transform → publish → sleep → repeat.
        
        Runs indefinitely until stopped (Ctrl+C) or too many errors.
        
        The loop:
        1. Fetches current flight data from OpenSky
        2. Transforms and publishes to Pub/Sub
        3. Sleeps for poll_interval seconds
        4. Repeats
        """
        logger.info("=" * 50)
        logger.info("Starting Flight Stream Producer")
        logger.info(f"Polling interval: {self.poll_interval} seconds")
        logger.info(f"Pub/Sub topic: {self.topic_path}")
        logger.info("=" * 50)
        
        while True:
            try:
                # Check for too many consecutive errors
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.error(
                        f"Stopping after {self.consecutive_errors} consecutive errors. "
                        "Check API availability and network connection."
                    )
                    break
                
                # Step 1: Fetch from API
                self.total_api_calls += 1
                raw_data = self.fetch_flights()
                
                if raw_data is None:
                    logger.info(f"Skipping this cycle (error #{self.consecutive_errors})")
                    time.sleep(self.poll_interval)
                    continue
                
                # Step 2: Transform and publish to Pub/Sub
                flight_count = self.transform_and_publish(raw_data)
                
                # Step 3: Wait before next poll
                logger.info(f"Cycle complete. Next poll in {self.poll_interval}s...")
                time.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Producer stopped by user (Ctrl+C)")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                self.consecutive_errors += 1
                time.sleep(self.poll_interval)
        
        # Final stats
        logger.info("=" * 50)
        logger.info("Producer Statistics:")
        logger.info(f"  Total API calls: {self.total_api_calls}")
        logger.info(f"  Total messages published: {self.total_messages_published}")
        logger.info("=" * 50)


if __name__ == "__main__":
    producer = FlightStreamProducer()
    producer.run()
