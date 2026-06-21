-- ════════════════════════════════════════════════════════════════
-- FlightTracker — Analysis Queries (Objetivo de Investigación 1)
-- ════════════════════════════════════════════════════════════════
--
-- OBJETIVO 1: "¿Cuáles son las rutas aéreas más congestionadas
--              por zona geográfica y horario del día?"
--
-- These queries analyze flight patterns using the cleaned data.
-- Run these in BigQuery console or connect to Looker Studio.
-- ════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────
-- Query 1: Top 10 countries with most active flights
-- Shows which countries have the most air traffic
-- ─────────────────────────────────────────────
SELECT
    origin_country,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT icao24) AS unique_aircraft,
    ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh,  -- m/s to km/h
    ROUND(AVG(baro_altitude), 0) AS avg_altitude_m
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false  -- Only airborne flights
  AND data_quality_flag = 'VALID'
GROUP BY origin_country
ORDER BY total_flights DESC
LIMIT 10;


-- ─────────────────────────────────────────────
-- Query 2: Flight distribution by hour of day
-- Shows peak hours for air traffic (useful for congestion analysis)
-- ─────────────────────────────────────────────
SELECT
    hour_of_day,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT origin_country) AS countries_represented,
    ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh,
    ROUND(AVG(baro_altitude), 0) AS avg_altitude_m
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
  AND data_quality_flag = 'VALID'
  AND hour_of_day IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ─────────────────────────────────────────────
-- Query 3: Flights by geographic region and altitude zone
-- Shows where flights cluster at different altitudes
-- ─────────────────────────────────────────────
SELECT
    geographic_region,
    altitude_zone,
    COUNT(*) AS flight_count,
    COUNT(DISTINCT icao24) AS unique_aircraft,
    ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
  AND data_quality_flag = 'VALID'
GROUP BY geographic_region, altitude_zone
ORDER BY geographic_region, flight_count DESC;


-- ─────────────────────────────────────────────
-- Query 4: Top 20 busiest "routes" (origin country pairs)
-- Simulates route congestion by grouping flights by origin
-- ─────────────────────────────────────────────
SELECT
    origin_country,
    geographic_region,
    COUNT(*) AS flight_count,
    ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh,
    ROUND(AVG(baro_altitude), 0) AS avg_altitude_m,
    -- Categorize congestion level
    CASE
        WHEN COUNT(*) > 500 THEN 'VERY_HIGH'
        WHEN COUNT(*) > 200 THEN 'HIGH'
        WHEN COUNT(*) > 50 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS congestion_level
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
  AND data_quality_flag = 'VALID'
GROUP BY origin_country, geographic_region
ORDER BY flight_count DESC
LIMIT 20;


-- ─────────────────────────────────────────────
-- Query 5: Speed distribution analysis
-- Shows how speed varies by altitude (expected: higher = faster)
-- ─────────────────────────────────────────────
SELECT
    altitude_zone,
    speed_category,
    COUNT(*) AS flight_count,
    ROUND(MIN(velocity) * 3.6, 1) AS min_speed_kmh,
    ROUND(MAX(velocity) * 3.6, 1) AS max_speed_kmh,
    ROUND(AVG(velocity) * 3.6, 1) AS avg_speed_kmh
FROM `flight_tracker.flights_cleaned`
WHERE on_ground = false
  AND velocity IS NOT NULL
  AND data_quality_flag = 'VALID'
GROUP BY altitude_zone, speed_category
ORDER BY altitude_zone, flight_count DESC;
