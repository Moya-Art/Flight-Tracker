-- ════════════════════════════════════════════════════════════════
-- FlightTracker — BigQuery ML Model (Objetivo de Investigación 2)
-- ════════════════════════════════════════════════════════════════
--
-- OBJETIVO 2: "¿Es posible detectar vuelos con comportamiento
--              anómalo utilizando técnicas de Machine Learning?"
--
-- We use BigQuery ML's KMeans algorithm to cluster flights
-- by their flight characteristics, then identify outliers.
--
-- What is KMeans?
-- It groups data points into K clusters based on similarity.
-- Flights that don't fit well into any cluster are potential anomalies.
-- ════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────
-- Step 1: Create the KMeans model
-- We cluster flights based on 3 features:
--   - velocity (ground speed)
--   - baro_altitude (barometric altitude)
--   - vertical_rate (climb/descent rate)
--
-- These features capture the "flight behavior" of an aircraft.
-- Normal flights cluster together; anomalies are far from centers.
-- ─────────────────────────────────────────────
CREATE OR REPLACE MODEL `flight_tracker.flight_anomaly_model`
OPTIONS(
    model_type='KMEANS',
    num_clusters=5,          -- 5 clusters: ground, low, cruise, high, extreme
    standardize_features=TRUE -- Normalize features so altitude doesn't dominate
)
AS SELECT
    velocity,
    baro_altitude,
    vertical_rate
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
  AND velocity IS NOT NULL
  AND baro_altitude IS NOT NULL
  AND vertical_rate IS NOT NULL
  AND data_quality_flag = 'VALID';


-- ─────────────────────────────────────────────
-- Step 2: Evaluate the model
-- Shows cluster sizes and centroids (center of each cluster)
-- ─────────────────────────────────────────────
SELECT
    *
FROM ML.EVALUATE(MODEL `flight_tracker.flight_anomaly_model`);


-- ─────────────────────────────────────────────
-- Step 3: See the cluster centroids
-- This shows what "typical" flights look like in each cluster
-- ─────────────────────────────────────────────
SELECT
    centroid_id,
    feature,
    ROUND(numerical_value, 2) AS center_value
FROM ML.CENTROIDS(MODEL `flight_tracker.flight_anomaly_model`)
ORDER BY centroid_id, feature;


-- ─────────────────────────────────────────────
-- Step 4: Predict clusters for all flights
-- and find ANOMALIES (flights far from their cluster center)
-- ─────────────────────────────────────────────
SELECT
    icao24,
    callsign,
    origin_country,
    velocity,
    baro_altitude,
    vertical_rate,
    geographic_region,
    predicted_cluster_id,
    -- Distance from cluster center (higher = more anomalous)
    ROUND(nearest_centroids_distance[OFFSET(0)].distance, 4) AS anomaly_score
FROM ML.PREDICT(
    MODEL `flight_tracker.flight_anomaly_model`,
    (
        SELECT
            icao24,
            callsign,
            origin_country,
            velocity,
            baro_altitude,
            vertical_rate,
            geographic_region
        FROM `flight_tracker.flights_cleaned`
        WHERE on_ground = false
          AND velocity IS NOT NULL
          AND baro_altitude IS NOT NULL
          AND vertical_rate IS NOT NULL
          AND data_quality_flag = 'VALID'
    )
)
ORDER BY anomaly_score DESC
LIMIT 50;


-- ─────────────────────────────────────────────
-- Step 5: Anomaly summary by cluster
-- Shows how many flights in each cluster and their characteristics
-- ─────────────────────────────────────────────
SELECT
    predicted_cluster_id AS cluster,
    COUNT(*) AS flight_count,
    ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh,
    ROUND(AVG(baro_altitude), 0) AS avg_altitude_m,
    ROUND(AVG(vertical_rate), 2) AS avg_vertical_rate,
    ROUND(AVG(nearest_centroids_distance[OFFSET(0)].distance), 4) AS avg_anomaly_score
FROM ML.PREDICT(
    MODEL `flight_tracker.flight_anomaly_model`,
    (
        SELECT
            velocity,
            baro_altitude,
            vertical_rate
        FROM `flight_tracker.flights_cleaned`
        WHERE on_ground = false
          AND velocity IS NOT NULL
          AND baro_altitude IS NOT NULL
          AND vertical_rate IS NOT NULL
          AND data_quality_flag = 'VALID'
    )
)
GROUP BY predicted_cluster_id
ORDER BY avg_anomaly_score DESC;
