-- ════════════════════════════════════════════════════════════════
-- FlightTracker — Data Cleaning & Transformation
-- Run this in BigQuery Console to create the cleaned table
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `flight_tracker.flights_cleaned`
AS SELECT
    -- Original fields
    icao24, callsign, origin_country, time_position, last_contact,
    longitude, latitude, baro_altitude, on_ground, velocity,
    true_track, vertical_rate, geo_altitude, squawk,
    ingestion_source, ingestion_timestamp,

    -- Speed category
    CASE
        WHEN velocity < 50 THEN 'SLOW'
        WHEN velocity < 200 THEN 'NORMAL'
        WHEN velocity < 350 THEN 'FAST'
        ELSE 'VERY_FAST'
    END AS speed_category,

    -- Altitude zone
    CASE
        WHEN on_ground = true THEN 'GROUND'
        WHEN baro_altitude < 3000 THEN 'LOW'
        WHEN baro_altitude < 9000 THEN 'MEDIUM'
        WHEN baro_altitude < 12000 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS altitude_zone,

    -- Geographic region
    CASE
        WHEN longitude BETWEEN -130 AND -60 AND latitude BETWEEN 10 AND 70 THEN 'NORTH_AMERICA'
        WHEN longitude BETWEEN -15 AND 45 AND latitude BETWEEN 35 AND 70 THEN 'EUROPE'
        WHEN longitude BETWEEN 60 AND 150 AND latitude BETWEEN -10 AND 55 THEN 'ASIA'
        WHEN longitude BETWEEN -80 AND -35 AND latitude BETWEEN -55 AND 15 THEN 'SOUTH_AMERICA'
        WHEN longitude BETWEEN -20 AND 55 AND latitude BETWEEN -35 AND 35 THEN 'AFRICA'
        WHEN longitude BETWEEN 110 AND 180 AND latitude BETWEEN -50 AND -10 THEN 'OCEANIA'
        ELSE 'OTHER'
    END AS geographic_region,

    -- Time features
    EXTRACT(HOUR FROM time_position) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM time_position) AS day_of_week,

    -- Data quality flag
    CASE
        WHEN icao24 IS NULL OR origin_country IS NULL THEN 'INVALID'
        WHEN longitude IS NULL AND latitude IS NULL THEN 'NO_POSITION'
        ELSE 'VALID'
    END AS data_quality_flag,

    CURRENT_TIMESTAMP() AS processed_at

FROM `flight_tracker.flights_raw`
WHERE icao24 IS NOT NULL
  AND origin_country IS NOT NULL
  AND longitude BETWEEN -180 AND 180
  AND latitude BETWEEN -90 AND 90;
