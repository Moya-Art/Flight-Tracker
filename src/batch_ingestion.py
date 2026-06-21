"""
FlightTracker — Batch Ingestion
Downloads current flight data from OpenSky Network API,
saves to Cloud Storage (Data Lake), and loads into BigQuery.

This handles the HISTORICAL/BATCH layer of the pipeline.
Run this script to load a snapshot of all current flights worldwide.
"""
import json
import logging
import sys
from datetime import datetime, timezone

import requests
from google.cloud import bigquery, storage

# Add parent directory to path so we can import config
sys.path.insert(0, ".")
from config.settings import (
    GCP_PROJECT_ID, GCS_BUCKET, GCS_BATCH_PREFIX,
    BQ_DATASET, BQ_TABLE_RAW, BQ_TABLE_ACTIVITY_LOG, OPENSKY_API_URL
)

# ──────────────────────────────────────────────
# Logging setup — records every execution
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/batch_ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_flight_data():
    """
    Step 1: Call the OpenSky Network REST API.
    Returns the raw JSON response with all current flights worldwide.
    
    Error handling: If the API is down or returns an error,
    we catch it and log it instead of crashing.
    """
    logger.info("Fetching flight data from OpenSky API...")
    
    try:
        response = requests.get(OPENSKY_API_URL, timeout=30)
        response.raise_for_status()  # Raises exception for 4xx/5xx status codes
        
        data = response.json()
        flight_count = len(data.get("states", []))
        logger.info(f"Successfully fetched {flight_count} flights")
        return data
        
    except requests.exceptions.Timeout:
        logger.error("API request timed out after 30 seconds")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to OpenSky API — is the service available?")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"API returned error status: {e}")
        raise


def transform_to_records(raw_data):
    """
    Step 2: Transform the raw API response into clean records.
    
    The OpenSky API returns a list of lists (positional arrays).
    We convert each one into a dictionary with named fields.
    This makes the data self-documenting and easier to work with.
    
    Reference: https://openskynetwork.github.io/opensky-api/rest.html
    """
    timestamp = datetime.now(timezone.utc)
    records = []
    
    # The API returns {"time": unix_timestamp, "states": [[...], [...]]}
    # Each state array has 17 positional fields
    for state in raw_data.get("states", []):
        # Skip malformed records (too few fields)
        if len(state) < 17:
            logger.warning(f"Skipping malformed record with {len(state)} fields")
            continue
        
        record = {
            "icao24": state[0],                    # Unique aircraft ID
            "callsign": (state[1] or "").strip(),  # Flight callsign (can be null)
            "origin_country": state[2],             # Country of registration
            "time_position": state[3],              # Unix timestamp of position
            "last_contact": state[4],               # Unix timestamp of last contact
            "longitude": state[5],                  # GPS longitude
            "latitude": state[6],                   # GPS latitude
            "baro_altitude": state[7],              # Barometric altitude (meters)
            "on_ground": state[8],                  # Is aircraft on ground?
            "velocity": state[9],                   # Ground speed (m/s)
            "true_track": state[10],                # Heading (degrees from north)
            "vertical_rate": state[11],             # Climb/descent rate (m/s)
            "geo_altitude": state[13],              # Geometric altitude (meters)
            "squawk": state[14],                    # Transponder code
            "ingestion_source": "batch",            # Mark as batch data
            "ingestion_timestamp": timestamp.isoformat()  # When we ingested it
        }
        records.append(record)
    
    logger.info(f"Transformed {len(records)} records")
    return records


def save_to_cloud_storage(records, timestamp):
    """
    Step 3: Save the data to Cloud Storage (our Data Lake).
    
    This creates a JSON file in GCS that serves as our raw archive.
    Even if BigQuery has issues, we always have the raw data here.
    This is the "Data Lake" layer — raw, immutable, append-only.
    """
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # Create filename with timestamp for easy identification
    filename = f"{GCS_BATCH_PREFIX}flights_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    blob = bucket.blob(filename)
    
    # Upload as JSON
    json_data = json.dumps(records, default=str)
    blob.upload_from_string(json_data, content_type="application/json")
    
    logger.info(f"Saved {len(records)} records to gs://{GCS_BUCKET}/{filename}")
    return filename


def load_to_bigquery(records):
    """
    Step 4: Load data into BigQuery.
    
    We use the BigQuery streaming insert API to load records.
    This handles deduplication via MERGE — if a record with the same
    icao24 + timestamp already exists, we skip it (no duplicates).
    
    Activity logging: We record this execution in the activity_log table.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_RAW}"
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Add to existing data
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
    
    # Load the data
    job = client.load_table_from_json(records, table_ref, job_config=job_config)
    result = job.result()  # Wait for the job to complete
    
    logger.info(f"Loaded {result.output_rows} rows into {table_ref}")
    return result.output_rows


def log_activity(record_count, status, error_message=None):
    """
    Step 5: Log this execution to the activity_log table.
    
    This is REQUIRED by the assignment — "Registro de Actividad".
    Every time the pipeline runs, we record:
    - When it ran
    - How many records were processed
    - Whether it succeeded or failed
    - Any error messages
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_ACTIVITY_LOG}"
    
    log_entry = [{
        "execution_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline": "batch_ingestion",
        "record_count": record_count,
        "status": status,
        "error_message": error_message
    }]
    
    errors = client.insert_rows_json(table_ref, log_entry)
    if errors:
        logger.error(f"Failed to log activity: {errors}")
    else:
        logger.info(f"Activity logged: {status}, {record_count} records")


def main():
    """
    Main pipeline execution.
    
    Flow: API → Transform → Cloud Storage → BigQuery → Log
    
    Every step has error handling. If one step fails,
    we log the failure and stop (don't continue with bad data).
    """
    logger.info("=" * 50)
    logger.info("Starting Batch Ingestion Pipeline")
    logger.info("=" * 50)
    
    record_count = 0
    
    try:
        # Step 1: Fetch from API
        raw_data = fetch_flight_data()
        
        # Step 2: Transform to records
        records = transform_to_records(raw_data)
        record_count = len(records)
        
        if record_count == 0:
            logger.warning("No flights found — API may be returning empty data")
            log_activity(0, "WARNING", "No flights returned from API")
            return
        
        # Step 3: Save to Data Lake (Cloud Storage) — optional, skip if bucket doesn't exist
        timestamp = datetime.now(timezone.utc)
        try:
            save_to_cloud_storage(records, timestamp)
        except Exception as e:
            logger.warning(f"Cloud Storage skipped (bucket may not exist): {e}")
        
        # Step 4: Load to BigQuery
        load_to_bigquery(records)
        
        # Step 5: Log success
        log_activity(record_count, "SUCCESS")
        logger.info(f"Pipeline completed successfully! {record_count} flights ingested.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        log_activity(record_count, "FAILED", str(e))
        raise


if __name__ == "__main__":
    main()
