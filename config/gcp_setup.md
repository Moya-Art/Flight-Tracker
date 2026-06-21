# GCP Lab Setup Guide — skills.google

Step-by-step instructions for setting up FlightTracker using a temporary GCP account from skills.google.

## How It Works

1. Go to https://www.skills.google/
2. Select a course with long duration (GCP, Big Data, etc.)
3. They give you a temporary user with GCP access
4. Use that access to run the pipeline

## During the Lab

### Step 1: Get Your Credentials

Once you're in the course environment:
- Note your **Project ID** (shown in the GCP console)
- Note how you authenticate (usually `gcloud auth login` or automatic)

### Step 2: Open Cloud Shell

1. Go to https://console.cloud.google.com/
2. Click the **Activate Cloud Shell** button (top right)
3. Wait for it to initialize

### Step 3: Clone the Repo

```bash
git clone https://github.com/Moya-Art/Flight-Tracker.git
cd Flight-Tracker
```

### Step 4: Create .env File

```bash
# Create .env with your project ID
cat > .env << 'EOF'
GCP_PROJECT_ID=YOUR_PROJECT_ID_HERE
GCP_REGION=us-central1
GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json
EOF
```

### Step 5: Authenticate (if needed)

```bash
# If the lab requires explicit auth:
gcloud auth login

# If it's automatic, skip this step

# Set the project
gcloud config set project YOUR_PROJECT_ID_HERE
```

### Step 6: Enable APIs

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com
```

### Step 7: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 8: Create Service Account (if needed)

Some labs require a service account for the Python scripts. If the automatic auth doesn't work:

```bash
# Create service account
gcloud iam service-accounts create flight-tracker-sa \
    --display-name "FlightTracker Service Account"

# Grant roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID_HERE \
    --member="serviceAccount:flight-tracker-sa@YOUR_PROJECT_ID_HERE.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID_HERE \
    --member="serviceAccount:flight-tracker-sa@YOUR_PROJECT_ID_HERE.iam.gserviceaccount.com" \
    --role="roles/pubsub.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID_HERE \
    --member="serviceAccount:flight-tracker-sa@YOUR_PROJECT_ID_HERE.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download key
gcloud iam service-accounts keys create config/service-account-key.json \
    --iam-account=flight-tracker-sa@YOUR_PROJECT_ID_HERE.iam.gserviceaccount.com

# Update .env
echo "GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json" >> .env
```

### Step 9: Run the Pipeline

```bash
# Option A: All at once (recommended)
python run_pipeline.py --stream-minutes 10

# Option B: Step by step
python setup.py
python src/batch_ingestion.py
# Terminal 1: python src/stream_ingestion.py
# Terminal 2: python src/subscriber.py
# (wait 10-15 min, then Ctrl+C both)
python src/data_cleaning.py
```

### Step 10: Run SQL Queries in BigQuery

1. Go to https://console.cloud.google.com/bigquery
2. Select your project
3. Copy and paste each query from `sql/queries.sql`
4. Run each query from `sql/ml_model.sql`
5. **TAKE SCREENSHOTS** of results

### Step 11: Create Dashboard in Looker Studio

1. Go to https://lookerstudio.google.com/
2. Create new report
3. Add data source → BigQuery → your project → `flight_tracker` → `flights_cleaned`
4. Create charts:
   - Table: Top 10 countries by flight count
   - Bar chart: Flights by hour of day
   - Geo map: Flights by geographic region
   - Scatter plot: Speed vs altitude (anomaly clusters)
5. **TAKE SCREENSHOTS** for your report

### Step 12: Export Results

- Download query results as CSV (BigQuery export button)
- Save dashboard screenshots
- Save any other evidence needed for the report

## After the Lab

- [ ] Write the report using the template
- [ ] Insert screenshots
- [ ] Create PowerPoint
- [ ] Submit to AVA

## Troubleshooting

### "Permission denied" on API calls
```bash
# Try with explicit credentials
export GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json
```

### "Project not found"
```bash
gcloud config set project YOUR_PROJECT_ID_HERE
```

### Python scripts can't find credentials
```bash
# Check .env exists and has correct values
cat .env

# Check the key file exists
ls -la config/service-account-key.json
```

### APIs not enabled
```bash
gcloud services enable bigquery.googleapis.com pubsub.googleapis.com storage.googleapis.com
```
