"""
FlightTracker — Data Cleaning & Transformation
Runs SQL transformations in BigQuery to clean raw data
and create the analytics-ready "cleaned" table.

This is the ETL (Extract, Transform, Load) layer.
We do transformations IN BigQuery using SQL — this is more efficient
than doing it in Python because BigQuery is optimized for large datasets.

What this script does:
1. Creates the flights_cleaned table (if not exists)
2. Deduplicates records (keeps latest by icao24 + timestamp)
3. Validates data (removes nulls, checks ranges)
4. Adds derived columns (speed category, altitude zone, time features)
5. Logs every execution
"""
import logging
import sys
from datetime import datetime, timezone

from google.cloud import bigquery

sys.path.insert(0, ".")
from config.settings import (
    GCP_PROJECT_ID, BQ_DATASET,
    BQ_TABLE_RAW, BQ_TABLE_CLEANED, BQ_TABLE_ACTIVITY_LOG
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/data_cleaning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# SQL Queries for Data Cleaning
# ──────────────────────────────────────────────

# Step 1: Create the cleaned table with derived columns
CREATE_CLEANED_TABLE = f"""
CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_CLEANED}`
AS SELECT
    -- Original fields
    icao24,
    callsign,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    geo_altitude,
    squawk,
    ingestion_source,
    ingestion_timestamp,

    -- ═══════════════════════════════════════════
    -- DERIVED COLUMNS (enrichment)
    -- These add business value for analysis
    -- ═══════════════════════════════════════════

    -- Speed category: useful for dashboards
    CASE
        WHEN velocity IS NULL THEN 'UNKNOWN'
        WHEN velocity < 50 THEN 'SLOW'          # < 180 km/h (taxiing/small aircraft)
        WHEN velocity < 200 THEN 'NORMAL'        # 180-720 km/h (commercial)
        WHEN velocity < 350 THEN 'FAST'          # 720-1260 km/h (fast commercial)
        ELSE 'VERY_FAST'                         # > 1260 km/h (military/supersonic)
    END AS speed_category,

    -- Altitude zone: useful for airspace analysis
    CASE
        WHEN on_ground = true THEN 'GROUND'
        WHEN baro_altitude IS NULL THEN 'UNKNOWN'
        WHEN baro_altitude < 3000 THEN 'LOW'       # < 3,000m (approach/departure)
        WHEN baro_altitude < 9000 THEN 'MEDIUM'    # 3,000-9,000m (regional)
        WHEN baro_altitude < 12000 THEN 'HIGH'     # 9,000-12,000m (cruising)
        ELSE 'VERY_HIGH'                           # > 12,000m (long-haul)
    END AS altitude_zone,

    -- Geographic region (simplified for analysis)
    CASE
        WHEN longitude BETWEEN -130 AND -60 AND latitude BETWEEN 10 AND 70 THEN 'NORTH_AMERICA'
        WHEN longitude BETWEEN -15 AND 45 AND latitude BETWEEN 35 AND 70 THEN 'EUROPE'
        WHEN longitude BETWEEN 60 AND 150 AND latitude BETWEEN -10 AND 55 THEN 'ASIA'
        WHEN longitude BETWEEN -80 AND -35 AND latitude BETWEEN -55 AND 15 THEN 'SOUTH_AMERICA'
        WHEN longitude BETWEEN -20 AND 55 AND latitude BETWEEN -35 AND 35 THEN 'AFRICA'
        WHEN longitude BETWEEN 110 AND 180 AND latitude BETWEEN -50 AND -10 THEN 'OCEANIA'
        ELSE 'OTHER'
    END AS geographic_region,

    -- Time features (for time-based analysis)
    EXTRACT(HOUR FROM time_position) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM time_position) AS day_of_week,

    -- Data quality flag
    CASE
        WHEN icao24 IS NULL OR origin_country IS NULL THEN 'INVALID'
        WHEN longitude IS NULL AND latitude IS NULL THEN 'NO_POSITION'
        WHEN velocity < 0 THEN 'INVALID_SPEED'
        ELSE 'VALID'
    END AS data_quality_flag,

    -- Processing metadata
    CURRENT_TIMESTAMP() AS processed_at

FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_RAW}`
WHERE icao24 IS NOT NULL
  AND origin_country IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90
"""

# Step 2: Deduplication — keep only latest record per aircraft
# Uses ROW_NUMBER() window function to identify duplicates
DEDUPLICATE_QUERY = f"""
MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_CLEANED}` T
USING (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY icao24, time_position
                ORDER BY ingestion_timestamp DESC
            ) AS rn
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_CLEANED}`
    )
    WHERE rn = 1  -- Keep only the most recent version
) S
ON T.icao24 = S.icao24 AND T.time_position = S.time_position
WHEN MATCHED AND T.ingestion_timestamp < S.ingestion_timestamp THEN
    UPDATE SET
        T.callsign = S.callsign,
        T.velocity = S.velocity,
        T.baro_altitude = S.baro_altitude,
        T.ingestion_timestamp = S.ingestion_timestamp
"""


def run_query(client, query, description):
    """Execute a BigQuery query and log the result."""
    logger.info(f"Running: {description}")
    
    try:
        job = client.query(query)
        result = job.result()
        
        # Get row count for logging
        rows_affected = job.num_dml_affected_rows if hasattr(job, 'num_dml_affected_rows') else 0
        logger.info(f"  ✓ {description} completed ({rows_affected} rows affected)")
        return rows_affected
        
    except Exception as e:
        logger.error(f"  ✗ {description} failed: {e}")
        raise


def log_activity(client, pipeline_step, record_count, status, error_message=None):
    """Log this execution to the activity_log table."""
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_ACTIVITY_LOG}"
    
    log_entry = [{
        "execution_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline": f"data_cleaning_{pipeline_step}",
        "record_count": record_count,
        "status": status,
        "error_message": error_message
    }]
    
    errors = client.insert_rows_json(table_ref, log_entry)
    if errors:
        logger.error(f"Failed to log activity: {errors}")


def main():
    """
    Main cleaning pipeline.
    
    Flow:
    1. Create cleaned table with derived columns
    2. Run deduplication
    3. Log everything
    """
    logger.info("=" * 50)
    logger.info("Starting Data Cleaning Pipeline")
    logger.info("=" * 50)
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    try:
        # Step 1: Create cleaned table with enrichment
        rows = run_query(client, CREATE_CLEANED_TABLE, "Create cleaned table with derived columns")
        log_activity(client, "transform", rows, "SUCCESS")
        
        # Step 2: Deduplicate
        rows = run_query(client, DEDUPLICATE_QUERY, "Deduplicate records")
        log_activity(client, "deduplicate", rows, "SUCCESS")
        
        logger.info("=" * 50)
        logger.info("Data cleaning completed successfully!")
        logger.info(f"  Cleaned table: {GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_CLEANED}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Cleaning pipeline failed: {e}")
        log_activity(client, "pipeline", 0, "FAILED", str(e))
        raise


if __name__ == "__main__":
    main()
