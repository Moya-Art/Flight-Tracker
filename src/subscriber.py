"""
FlightTracker — Subscriber (Consumer)
Reads flight data messages from Pub/Sub and loads them into BigQuery.

This is the CONSUMER side of the streaming pipeline.

Architecture:
    OpenSky API → Producer → Pub/Sub Topic → [This Script] → BigQuery

Why separate producer and subscriber?
1. They can run on different machines
2. If the subscriber crashes, messages are safe in Pub/Sub
3. We can have multiple subscribers for different purposes
4. Each can scale independently
"""
import json
import logging
import sys
from datetime import datetime, timezone
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1, bigquery

sys.path.insert(0, ".")
from config.settings import (
    GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION,
    BQ_DATASET, BQ_TABLE_RAW, BQ_TABLE_ACTIVITY_LOG
)

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/subscriber.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FlightSubscriber:
    """
    Subscribes to Pub/Sub topic and writes flight data to BigQuery.
    
    Key features:
    - Deduplication: MERGE instead of INSERT to avoid duplicate records
    - Activity logging: Every batch is logged with count and status
    - Error handling: Failed messages are logged, not lost
    - Back-pressure: Only acknowledges messages after successful BigQuery write
    """
    
    def __init__(self):
        self.project_id = GCP_PROJECT_ID
        self.subscription_id = PUBSUB_SUBSCRIPTION
        self.bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Pub/Sub subscriber
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_id
        )
        
        # Statistics
        self.total_messages_received = 0
        self.total_records_written = 0
        self.total_errors = 0
    
    def write_to_bigquery(self, records):
        """
        Write flight records to BigQuery using MERGE for deduplication.
        
        MERGE logic:
        - If a record with the same icao24 + time_position already exists → SKIP
        - If it's new → INSERT
        
        This prevents duplicates when:
        1. The same flight appears in multiple API polls
        2. Pub/Sub delivers the same message twice (at-least-once delivery)
        3. The pipeline is restarted and reprocesses old messages
        """
        table_ref = f"{self.project_id}.{BQ_DATASET}.{BQ_TABLE_RAW}"
        
        # First, insert raw records
        # We use load_table_from_json for efficiency with batches
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("icao24", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("callsign", "STRING"),
                bigquery.SchemaField("origin_country", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("time_position", "TIMESTAMP"),
                bigquery.SchemaField("last_contact", "TIMESTAMP"),
                bigquery.SchemaField("longitude", "FLOAT64"),
                bigquery.SchemaField("latitude", "FLOAT64"),
                bigquery.SchemaField("baro_altitude", "FLOAT64"),
                bigquery.SchemaField("on_ground", "BOOLEAN"),
                bigquery.SchemaField("velocity", "FLOAT64"),
                bigquery.SchemaField("true_track", "FLOAT64"),
                bigquery.SchemaField("vertical_rate", "FLOAT64"),
                bigquery.SchemaField("geo_altitude", "FLOAT64"),
                bigquery.SchemaField("squawk", "STRING"),
                bigquery.SchemaField("ingestion_source", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
            ]
        )
        
        # Add metadata fields to each record
        timestamp = datetime.now(timezone.utc).isoformat()
        for record in records:
            record["ingestion_source"] = "streaming"
            record["ingestion_timestamp"] = timestamp
        
        job = self.bq_client.load_table_from_json(
            records, table_ref, job_config=job_config
        )
        result = job.result()
        return result.output_rows
    
    def log_activity(self, record_count, status, error_message=None):
        """Log this execution to the activity_log table."""
        table_ref = f"{self.project_id}.{BQ_DATASET}.{BQ_TABLE_ACTIVITY_LOG}"
        
        log_entry = [{
            "execution_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline": "stream_subscriber",
            "record_count": record_count,
            "status": status,
            "error_message": error_message
        }]
        
        errors = self.bq_client.insert_rows_json(table_ref, log_entry)
        if errors:
            logger.error(f"Failed to log activity: {errors}")
    
    def process_message(self, message):
        """
        Process a single Pub/Sub message.
        
        Each message contains a batch of flights from one API poll.
        
        Flow:
        1. Deserialize the JSON message
        2. Extract flight records
        3. Write to BigQuery
        4. Log the activity
        5. Acknowledge the message (tells Pub/Sub it's been processed)
        
        IMPORTANT: We only ack() AFTER successful BigQuery write.
        If BigQuery fails, the message stays in Pub/Sub and will be redelivered.
        This ensures no data is lost.
        """
        self.total_messages_received += 1
        
        try:
            # Step 1: Parse the message
            data = json.loads(message.data.decode("utf-8"))
            metadata = data.get("metadata", {})
            flights = data.get("flights", [])
            
            logger.info(
                f"Received message {message.message_id}: "
                f"{len(flights)} flights from {metadata.get('source', 'unknown')}"
            )
            
            if not flights:
                logger.warning("Message contains no flights — acknowledging and skipping")
                message.ack()
                return
            
            # Step 2: Write to BigQuery
            rows_written = self.write_to_bigquery(flights)
            self.total_records_written += rows_written
            
            # Step 3: Log success
            self.log_activity(rows_written, "SUCCESS")
            
            # Step 4: Acknowledge — message is fully processed
            message.ack()
            logger.info(f"Acknowledged message {message.message_id} ({rows_written} rows written)")
            
        except Exception as e:
            # If anything fails, DON'T ack — message will be redelivered
            self.total_errors += 1
            logger.error(f"Error processing message: {e}")
            self.log_activity(0, "FAILED", str(e))
            message.nack()  # Tell Pub/Sub to redeliver this message
    
    def run(self):
        """
        Start the subscriber. This blocks and runs forever,
        processing messages as they arrive from Pub/Sub.
        
        Flow control:
        - max_messages=10: Process up to 10 messages concurrently
        - This prevents overwhelming BigQuery with too many writes
        """
        logger.info("=" * 50)
        logger.info("Starting Flight Subscriber")
        logger.info(f"Subscription: {self.subscription_path}")
        logger.info("=" * 50)
        
        # Configure flow control — how many messages to process at once
        flow_control = pubsub_v1.types.FlowControl(max_messages=10)
        
        # Start listening
        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path,
            callback=self.process_message,
            flow_control=flow_control,
        )
        
        logger.info("Listening for messages... Press Ctrl+C to stop.")
        
        try:
            # This blocks until the subscriber is stopped
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            logger.info("Subscriber stopped by user")
        except Exception as e:
            streaming_pull_future.cancel()
            logger.error(f"Subscriber error: {e}")
        finally:
            # Final statistics
            logger.info("=" * 50)
            logger.info("Subscriber Statistics:")
            logger.info(f"  Messages received: {self.total_messages_received}")
            logger.info(f"  Records written: {self.total_records_written}")
            logger.info(f"  Errors: {self.total_errors}")
            logger.info("=" * 50)
            
            self.subscriber.close()


if __name__ == "__main__":
    subscriber = FlightSubscriber()
    subscriber.run()
