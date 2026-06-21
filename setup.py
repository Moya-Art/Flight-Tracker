"""
FlightTracker — GCP Setup Script
Run this ONCE in your GCP lab to create all the infrastructure.

This script creates:
1. BigQuery dataset and tables
2. Pub/Sub topic and subscription
3. Cloud Storage bucket

Usage:
    python setup.py
"""
import logging
import sys

from google.cloud import bigquery, pubsub_v1, storage

sys.path.insert(0, ".")
from config.settings import (
    GCP_PROJECT_ID, GCS_BUCKET,
    BQ_DATASET, BQ_TABLE_RAW, BQ_TABLE_CLEANED, BQ_TABLE_ACTIVITY_LOG,
    PUBSUB_TOPIC, PUBSUB_SUBSCRIPTION
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_bigquery_dataset_and_tables():
    """Create the BigQuery dataset and all required tables."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # Create dataset
    dataset_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset.description = "FlightTracker — Real-time air traffic analytics"
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info(f"✓ Dataset '{BQ_DATASET}' ready")
    except Exception as e:
        logger.error(f"✗ Failed to create dataset: {e}")
        raise
    
    # Create flights_raw table
    raw_schema = [
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
    
    raw_table = bigquery.Table(f"{dataset_id}.{BQ_TABLE_RAW}", schema=raw_schema)
    raw_table.description = "Raw flight data from OpenSky API (batch + streaming)"
    raw_table = client.create_table(raw_table, exists_ok=True)
    logger.info(f"✓ Table '{BQ_TABLE_RAW}' ready")
    
    # Create activity_log table
    log_schema = [
        bigquery.SchemaField("execution_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("pipeline", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("record_count", "INTEGER"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("error_message", "STRING"),
    ]
    
    log_table = bigquery.Table(f"{dataset_id}.{BQ_TABLE_ACTIVITY_LOG}", schema=log_schema)
    log_table.description = "Pipeline execution tracking and logging"
    log_table = client.create_table(log_table, exists_ok=True)
    logger.info(f"✓ Table '{BQ_TABLE_ACTIVITY_LOG}' ready")


def create_pubsub():
    """Create Pub/Sub topic and subscription."""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)
    subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION)
    
    # Create topic
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        logger.info(f"✓ Topic '{PUBSUB_TOPIC}' created")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            logger.info(f"✓ Topic '{PUBSUB_TOPIC}' already exists")
        else:
            raise
    
    # Create subscription
    try:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 60,  # 60 seconds to process each message
            }
        )
        logger.info(f"✓ Subscription '{PUBSUB_SUBSCRIPTION}' created")
    except Exception as e:
        if "ALREADY_EXISTS" in str(e):
            logger.info(f"✓ Subscription '{PUBSUB_SUBSCRIPTION}' already exists")
        else:
            raise


def create_cloud_storage_bucket():
    """Create the Cloud Storage bucket for the Data Lake."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    
    try:
        bucket = storage_client.create_bucket(GCS_BUCKET, location="US")
        logger.info(f"✓ Bucket 'gs://{GCS_BUCKET}' created")
    except Exception as e:
        if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
            logger.info(f"✓ Bucket 'gs://{GCS_BUCKET}' already exists")
        else:
            raise


def main():
    """Run all setup steps."""
    logger.info("=" * 50)
    logger.info("FlightTracker — GCP Setup")
    logger.info(f"Project: {GCP_PROJECT_ID}")
    logger.info("=" * 50)
    
    try:
        create_bigquery_dataset_and_tables()
        create_pubsub()
        create_cloud_storage_bucket()
        
        logger.info("=" * 50)
        logger.info("✓ All GCP resources created successfully!")
        logger.info("=" * 50)
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Set GCP_PROJECT_ID in config/settings.py")
        logger.info("  2. Set GOOGLE_APPLICATION_CREDENTIALS env var")
        logger.info("  3. Run: python src/batch_ingestion.py")
        logger.info("  4. Run: python src/stream_ingestion.py")
        logger.info("  5. Run: python src/subscriber.py (in another terminal)")
        logger.info("  6. Run: python src/data_cleaning.py")
        logger.info("  7. Open BigQuery console and run sql/queries.sql")
        logger.info("  8. Open BigQuery console and run sql/ml_model.sql")
        logger.info("  9. Connect Looker Studio to BigQuery for dashboard")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise


if __name__ == "__main__":
    main()
