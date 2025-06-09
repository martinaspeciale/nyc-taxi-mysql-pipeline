-- eda_views/views.sql
-- EDA views for NYC Taxi project
-- Used in Tableau dashboards and Python EDA (eda_results/)
-- Data source: yellow_taxi_trips table

-- 1️⃣ Rides per day
CREATE OR REPLACE VIEW rides_per_day AS
SELECT
    DATE(tpep_pickup_datetime) AS ride_date,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips
GROUP BY
    ride_date
ORDER BY
    ride_date;

-- 2️⃣ Rides per hour of day
CREATE OR REPLACE VIEW rides_per_hour_of_day AS
SELECT
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips
GROUP BY
    pickup_hour
ORDER BY
    pickup_hour;

-- 3️⃣ Pickups per location
CREATE OR REPLACE VIEW pickups_per_location AS
SELECT
    pu_location_id,
    COUNT(*) AS total_pickups,
    SUM(total_amount) AS total_revenue
FROM
    yellow_taxi_trips
GROUP BY
    pu_location_id
ORDER BY
    total_pickups DESC;

-- 4️⃣ Fare vs. trip distance
CREATE OR REPLACE VIEW fare_vs_distance AS
SELECT
    trip_distance,
    total_amount
FROM
    yellow_taxi_trips
WHERE
    trip_distance > 0 AND trip_distance < 50
    AND total_amount > 0 AND total_amount < 200;

-- 5️⃣ Rides by passenger count
CREATE OR REPLACE VIEW rides_by_passenger_count AS
SELECT
    passenger_count,
    COUNT(*) AS total_rides,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips
GROUP BY
    passenger_count
ORDER BY
    passenger_count;

-- 6️⃣ Revenue pickup → dropoff matrix
CREATE OR REPLACE VIEW revenue_pickup_dropoff_matrix AS
SELECT
    pu_location_id,
    do_location_id,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips
GROUP BY
    pu_location_id, do_location_id
ORDER BY
    total_revenue DESC;

-- 7️⃣ Distance by hour of day
CREATE OR REPLACE VIEW distance_by_hour AS
SELECT
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    AVG(trip_distance) AS avg_distance,
    MAX(trip_distance) AS max_distance,
    MIN(trip_distance) AS min_distance,
    COUNT(*) AS total_rides
FROM
    yellow_taxi_trips
GROUP BY
    pickup_hour
ORDER BY
    pickup_hour;
