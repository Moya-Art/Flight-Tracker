"""
FlightTracker — Central Configuration
All GCP and API settings in one place.
Change these values to match YOUR GCP project.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# Google Cloud Platform
# ──────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-project-id")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")

# Path to your service account JSON key
# NEVER commit this file to GitHub
GCP_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "config/service-account-key.json")

# ──────────────────────────────────────────────
# BigQuery
# ──────────────────────────────────────────────
BQ_DATASET = "flight_tracker"
BQ_TABLE_RAW = "flights_raw"           # Raw data from API (batch + stream land here)
BQ_TABLE_CLEANED = "flights_cleaned"   # Cleaned, validated, deduplicated
BQ_TABLE_ACTIVITY_LOG = "activity_log" # Execution tracking

# ──────────────────────────────────────────────
# Cloud Storage (Data Lake)
# ──────────────────────────────────────────────
GCS_BUCKET = f"{GCP_PROJECT_ID}-flight-data"
GCS_BATCH_PREFIX = "batch/"  # Folder for batch files in the bucket

# ──────────────────────────────────────────────
# Pub/Sub
# ──────────────────────────────────────────────
PUBSUB_TOPIC = "flight-updates"
PUBSUB_SUBSCRIPTION = "flight-updates-sub"

# ──────────────────────────────────────────────
# OpenSky Network API
# ──────────────────────────────────────────────
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
OPENSKY_POLL_INTERVAL = 15  # seconds between API calls (free tier limit)
