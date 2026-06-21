# GCP Lab Setup Guide

Step-by-step instructions for setting up FlightTracker in a Google Cloud Skills Boost lab.

## Before the Lab

Make sure you have:
- [ ] All Python files written and ready
- [ ] `requirements.txt` ready
- [ ] `setup.py` and `run_pipeline.py` ready
- [ ] Know your GCP project ID (you'll get it when the lab starts)

## During the Lab (1-1.5 hours)

### Step 1: Open Cloud Shell (2 min)

1. Click the **Activate Cloud Shell** button (top right of GCP console)
2. Wait for the shell to initialize
3. Clone your repo or upload files:

```bash
# If using GitHub:
git clone https://github.com/Moya-Art/Flight-Tracker.git
cd Flight-Tracker

# If uploading files:
# Use the Cloud Shell "Upload" button
```

### Step 2: Get Your Project ID (1 min)

```bash
# The lab will give you a project ID, or:
gcloud config get-value project
```

### Step 3: Create Service Account and Key (3 min)

```bash
# Replace with YOUR project ID from the lab
export PROJECT_ID="your-lab-project-id"

# Create a service account
gcloud iam service-accounts create flight-tracker-sa \
    --display-name "FlightTracker Service Account"

# Grant roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download the key
gcloud iam service-accounts keys create config/service-account-key.json \
    --iam-account=flight-tracker-sa@$PROJECT_ID.iam.gserviceaccount.com
```

### Step 4: Create .env File (1 min)

```bash
# Create .env from template
cp .env.example .env

# Edit with your project ID
echo "GCP_PROJECT_ID=$PROJECT_ID" > .env
echo "GCP_REGION=us-central1" >> .env
echo "GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json" >> .env
```

### Step 5: Enable APIs (1 min)

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com
```

### Step 6: Install Dependencies (2 min)

```bash
pip install -r requirements.txt
```

### Step 7: Run the Full Pipeline (20-30 min)

**Option A: Run everything at once (recommended)**
```bash
# Runs: setup → batch ingestion → streaming (10 min) → cleaning
python run_pipeline.py --stream-minutes 10
```

**Option B: Run step by step**
```bash
# 1. Setup GCP infrastructure
python setup.py

# 2. Batch ingestion (historical data)
python src/batch_ingestion.py

# 3. Streaming (open TWO terminals)
# Terminal 1:
python src/stream_ingestion.py
# Terminal 2:
python src/subscriber.py
# Let it run for 10-15 minutes, then Ctrl+C both

# 4. Data cleaning
python src/data_cleaning.py
```

### Step 8: Run SQL Queries (10 min)

1. Go to [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Run each query from `sql/queries.sql`
3. Run each query from `sql/ml_model.sql`
4. **TAKE SCREENSHOTS** of the results for your report

### Step 9: Create Dashboard (15 min)

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Create new report
3. Add BigQuery data source → `flight_tracker.flights_cleaned`
4. Create these charts:
   - **Table:** Top 10 countries by flight count
   - **Bar chart:** Flights by hour of day
   - **Geo map:** Flights by geographic region
   - **Scatter plot:** Speed vs altitude (showing anomaly clusters)
5. **TAKE SCREENSHOTS** for your report

### Step 10: Export for Report (5 min)

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
- [ ] Push code to GitHub (if not already done)

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

### .env not loading
```bash
# Make sure python-dotenv is installed
pip install python-dotenv

# Check that .env exists
cat .env
```
