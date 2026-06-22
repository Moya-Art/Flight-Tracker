#!/bin/bash
# FlightTracker — Deploy to Cloud Run
# Run this script in Cloud Shell after setting up the project

set -e

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
SERVICE_NAME="flight-tracker-producer"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "╔══════════════════════════════════════════════════╗"
echo "║  FlightTracker — Cloud Run Deployment            ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Service: ${SERVICE_NAME}"
echo ""

# ──────────────────────────────────────────────
# Step 1: Enable required APIs
# ──────────────────────────────────────────────
echo "Step 1: Enabling APIs..."
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable bigquery.googleapis.com
echo "✓ APIs enabled"
echo ""

# ──────────────────────────────────────────────
# Step 2: Create Pub/Sub topic (if not exists)
# ──────────────────────────────────────────────
echo "Step 2: Creating Pub/Sub topic..."
gcloud pubsub topics create flight-updates --quiet 2>/dev/null || echo "Topic already exists"
gcloud pubsub subscriptions create flight-updates-sub \
    --topic=flight-updates \
    --ack-deadline=60 \
    --quiet 2>/dev/null || echo "Subscription already exists"
echo "✓ Pub/Sub ready"
echo ""

# ──────────────────────────────────────────────
# Step 3: Create BigQuery dataset and tables
# ──────────────────────────────────────────────
echo "Step 3: Creating BigQuery dataset..."
bq mk --dataset --location=US ${PROJECT_ID}:flight_tracker 2>/dev/null || echo "Dataset already exists"

# Create the raw table
bq mk --table \
    ${PROJECT_ID}:flight_tracker.flights_raw \
    icao24:STRING,callsign:STRING,origin_country:STRING,\
time_position:TIMESTAMP,last_contact:TIMESTAMP,\
longitude:FLOAT,latitude:FLOAT,baro_altitude:FLOAT,\
on_ground:BOOLEAN,velocity:FLOAT,true_track:FLOAT,\
vertical_rate:FLOAT,geo_altitude:FLOAT,squawk:STRING,\
ingestion_source:STRING,ingestion_timestamp:TIMESTAMP \
    2>/dev/null || echo "Table already exists"

# Create activity log table
bq mk --table \
    ${PROJECT_ID}:flight_tracker.activity_log \
    execution_timestamp:TIMESTAMP,pipeline:STRING,\
record_count:INTEGER,status:STRING,error_message:STRING \
    2>/dev/null || echo "Table already exists"

echo "✓ BigQuery ready"
echo ""

# ──────────────────────────────────────────────
# Step 4: Build and push Docker image
# ──────────────────────────────────────────────
echo "Step 4: Building Docker image..."
gcloud builds submit --tag ${IMAGE_NAME}
echo "✓ Image built and pushed"
echo ""

# ──────────────────────────────────────────────
# Step 5: Deploy to Cloud Run
# ──────────────────────────────────────────────
echo "Step 5: Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME} \
    --region ${REGION} \
    --platform managed \
    --no-allow-unauthenticated \
    --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},PUBSUB_TOPIC=flight-updates,POLL_INTERVAL=15,MAX_DURATION=600" \
    --memory 512Mi \
    --cpu 1 \
    --timeout 900 \
    --min-instances 1 \
    --max-instances 1
echo "✓ Service deployed"
echo ""

# ──────────────────────────────────────────────
# Step 6: Get service URL
# ──────────────────────────────────────────────
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format 'value(status.url)')
echo "╔══════════════════════════════════════════════════╗"
echo "║  Deployment Complete!                            ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Service URL: ${SERVICE_URL}"
echo ""
echo "To check status: curl ${SERVICE_URL}"
echo "To view logs: gcloud run services logs read ${SERVICE_NAME} --region ${REGION}"
echo ""
echo "The producer will run for 10 minutes, publishing flights to Pub/Sub."
echo "After 10 minutes, check BigQuery for the data."
