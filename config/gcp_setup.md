# GCP Lab Setup Guide

Step-by-step instructions for setting up FlightTracker in a Google Cloud Skills Boost lab.

## Before the Lab

Make sure you have:
- [ ] All Python files written and ready
- [ ] `requirements.txt` ready
- [ ] `setup.py` ready
- [ ] Know your GCP project ID (you'll get it when the lab starts)

## During the Lab (1-1.5 hours)

### Step 1: Open Cloud Shell (2 min)

1. Click the **Activate Cloud Shell** button (top right of GCP console)
2. Wait for the shell to initialize
3. Clone your repo or upload files:

```bash
# If using GitHub:
git clone https://github.com/Moya-Art/flight-tracker.git
cd flight-tracker

# If uploading files:
# Use the Cloud Shell "Upload" button
```

### Step 2: Get Your Project ID (1 min)

```bash
# The lab will give you a project ID, or:
gcloud config get-value project
```

### Step 3: Set Environment Variables (1 min)

```bash
# Replace with YOUR project ID from the lab
export GCP_PROJECT_ID="your-lab-project-id"

# Create a service account key
gcloud iam service-accounts create flight-tracker-sa \
    --display-name "FlightTracker Service Account"

# Grant roles
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.admin"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download the key
gcloud iam service-accounts keys create config/service-account-key.json \
    --iam-account=flight-tracker-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com

# Set the credentials environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/config/service-account-key.json"
```

### Step 4: Enable APIs (1 min)

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com
```

### Step 5: Install Dependencies (2 min)

```bash
pip install -r requirements.txt
```

### Step 6: Run Setup Script (2 min)

```bash
python setup.py
```

This creates:
- BigQuery dataset `flight_tracker` with tables
- Pub/Sub topic `flight-updates` and subscription
- Cloud Storage bucket

### Step 7: Run Batch Ingestion (5 min)

```bash
python src/batch_ingestion.py
```

This downloads current flight data and loads it into BigQuery.
Check BigQuery console to see the data.

### Step 8: Run Streaming (15-20 min)

Open TWO terminals:

**Terminal 1 — Producer:**
```bash
python src/stream_ingestion.py
```

**Terminal 2 — Subscriber:**
```bash
python src/subscriber.py
```

Let it run for 10-15 minutes to collect streaming data.

### Step 9: Run Data Cleaning (2 min)

```bash
python src/data_cleaning.py
```

### Step 10: Run SQL Queries (10 min)

1. Go to BigQuery Console
2. Run each query from `sql/queries.sql`
3. Run each query from `sql/ml_model.sql`
4. **TAKE SCREENSHOTS** of the results for your report

### Step 11: Create Dashboard (15 min)

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Create new report
3. Add BigQuery data source → `flight_tracker.flights_cleaned`
4. Create these charts:
   - **Table:** Top 10 countries by flight count
   - **Bar chart:** Flights by hour of day
   - **Geo map:** Flights by geographic region
   - **Scatter plot:** Speed vs altitude (showing anomaly clusters)
5. **TAKE SCREENSHOTS** for your report

### Step 12: Export for Report (5 min)

```bash
# Download query results as CSV for the report
# (Use BigQuery console export button)

# Download screenshots
# (Right-click → Save Image in Looker Studio)
```

## After the Lab

- [ ] Write the report using the template
- [ ] Insert screenshots into the report
- [ ] Create PowerPoint presentation
- [ ] Push code to GitHub

## Troubleshooting

### "Project not found"
```bash
gcloud config set project YOUR_PROJECT_ID
```

### "Permission denied"
```bash
# Make sure you're authenticated
gcloud auth application-default login
```

### "API not enabled"
```bash
gcloud services enable bigquery.googleapis.com pubsub.googleapis.com storage.googleapis.com
```

### Pub/Sub not receiving messages
- Check that the producer is running
- Check that the subscription exists
- Check logs for errors

### BigQuery table not found
```bash
# Re-run setup
python setup.py
```
