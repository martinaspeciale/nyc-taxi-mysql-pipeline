-- eda_views/views_september.sql
-- EDA views for NYC Taxi project — September comparison (multi-year)
-- Used in Tableau dashboards and Python EDA (eda_results/)
-- Data source: yellow_taxi_trips_september_comparison table

-- 1️⃣ Rides per day
CREATE OR REPLACE VIEW rides_per_day_september AS
SELECT
    DATE(tpep_pickup_datetime) AS ride_date,
    source_year,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, ride_date
ORDER BY
    source_year, ride_date;

-- 2️⃣ Rides per hour of day
CREATE OR REPLACE VIEW rides_per_hour_of_day_september AS
SELECT
    source_year,
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, pickup_hour
ORDER BY
    source_year, pickup_hour;

-- 3️⃣ Pickups per location
CREATE OR REPLACE VIEW pickups_per_location_september AS
SELECT
    source_year,
    pu_location_id,
    COUNT(*) AS total_pickups,
    SUM(total_amount) AS total_revenue
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, pu_location_id
ORDER BY
    source_year, total_pickups DESC;

-- 4️⃣ Fare vs. trip distance
CREATE OR REPLACE VIEW fare_vs_distance_september AS
SELECT
    source_year,
    trip_distance,
    total_amount
FROM
    yellow_taxi_trips_september_comparison
WHERE
    trip_distance > 0 AND trip_distance < 50
    AND total_amount > 0 AND total_amount < 200;

-- 5️⃣ Rides by passenger count
CREATE OR REPLACE VIEW rides_by_passenger_count_september AS
SELECT
    source_year,
    passenger_count,
    COUNT(*) AS total_rides,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, passenger_count
ORDER BY
    source_year, passenger_count;

-- 6️⃣ Revenue pickup → dropoff matrix
CREATE OR REPLACE VIEW revenue_pickup_dropoff_matrix_september AS
SELECT
    source_year,
    pu_location_id,
    do_location_id,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, pu_location_id, do_location_id
ORDER BY
    source_year, total_revenue DESC;

-- 7️⃣ Distance by hour of day
CREATE OR REPLACE VIEW distance_by_hour_september AS
SELECT
    source_year,
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    AVG(trip_distance) AS avg_distance,
    MAX(trip_distance) AS max_distance,
    MIN(trip_distance) AS min_distance,
    COUNT(*) AS total_rides
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, pickup_hour
ORDER BY
    source_year, pickup_hour;

-- 8️⃣ Pickups per week and location
CREATE OR REPLACE VIEW pickups_per_week_location_september AS
SELECT
    source_year,
    WEEK(tpep_pickup_datetime, 3) AS pickup_week,
    pu_location_id,
    COUNT(*) AS total_pickups,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM
    yellow_taxi_trips_september_comparison
GROUP BY
    source_year, pickup_week, pu_location_id
ORDER BY
    source_year, pickup_week, pu_location_id;

-- 9️⃣ Rides per week + passenger count
CREATE OR REPLACE VIEW rides_per_week_passenger_count_september AS
SELECT
    source_year,
    WEEK(tpep_pickup_datetime, 3) AS pickup_week,
    CONCAT(source_year, '-W', LPAD(WEEK(tpep_pickup_datetime, 3), 2, '0')) AS pickup_yearweek,
    passenger_count,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM yellow_taxi_trips_september_comparison
GROUP BY source_year, pickup_week, pickup_yearweek, passenger_count
ORDER BY source_year, pickup_week, passenger_count;

-- 1️⃣0️⃣ Revenue trend per payment type
CREATE OR REPLACE VIEW revenue_per_week_payment_type_september AS
SELECT
    source_year,
    WEEK(tpep_pickup_datetime, 3) AS pickup_week,
    CONCAT(source_year, '-W', LPAD(WEEK(tpep_pickup_datetime, 3), 2, '0')) AS pickup_yearweek,
    payment_type,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM yellow_taxi_trips_september_comparison
GROUP BY source_year, pickup_week, pickup_yearweek, payment_type
ORDER BY source_year, pickup_week, payment_type;

-- 1️⃣1️⃣ Tip percentage trend
CREATE OR REPLACE VIEW tip_percentage_per_week_september AS
SELECT
    source_year,
    WEEK(tpep_pickup_datetime, 3) AS pickup_week,
    CONCAT(source_year, '-W', LPAD(WEEK(tpep_pickup_datetime, 3), 2, '0')) AS pickup_yearweek,
    AVG(CASE WHEN total_amount > 0 THEN tip_amount / total_amount ELSE 0 END) * 100 AS avg_tip_percentage
FROM yellow_taxi_trips_september_comparison
GROUP BY source_year, pickup_week, pickup_yearweek
ORDER BY source_year, pickup_week;

-- 1️⃣2️⃣ Trip distance buckets
CREATE OR REPLACE VIEW trip_distance_buckets_september AS
SELECT
    source_year,
    CASE
        WHEN trip_distance < 1 THEN '<1 mile'
        WHEN trip_distance < 2 THEN '1-2 miles'
        WHEN trip_distance < 5 THEN '2-5 miles'
        WHEN trip_distance < 10 THEN '5-10 miles'
        WHEN trip_distance < 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END AS distance_bucket,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM yellow_taxi_trips_september_comparison
WHERE trip_distance > 0 AND total_amount > 0
GROUP BY source_year, distance_bucket
ORDER BY source_year, FIELD(distance_bucket, '<1 mile', '1-2 miles', '2-5 miles', '5-10 miles', '10-20 miles', '20+ miles');

-- 1️⃣3️⃣ Revenue per pickup hour → dropoff hour matrix
CREATE OR REPLACE VIEW revenue_pickup_dropoff_hour_matrix_september AS
SELECT
    source_year,
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    HOUR(tpep_dropoff_datetime) AS dropoff_hour,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM yellow_taxi_trips_september_comparison
GROUP BY source_year, pickup_hour, dropoff_hour
ORDER BY source_year, pickup_hour, dropoff_hour;

-- 1️⃣4️⃣ Trips with highest tip % → top 1% percentile
CREATE OR REPLACE VIEW top_tipping_trips_september AS
SELECT
    source_year,
    tpep_pickup_datetime,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    total_amount,
    tip_amount,
    (tip_amount / total_amount) * 100 AS tip_percentage
FROM yellow_taxi_trips_september_comparison
WHERE total_amount > 0 AND tip_amount > 0
ORDER BY tip_percentage DESC
LIMIT 1000;

-- 1️⃣5️⃣ Passenger count X trip distance → behavior grid
CREATE OR REPLACE VIEW passenger_count_trip_distance_grid_september AS
SELECT
    source_year,
    passenger_count,
    CASE
        WHEN trip_distance < 1 THEN '<1 mile'
        WHEN trip_distance < 2 THEN '1-2 miles'
        WHEN trip_distance < 5 THEN '2-5 miles'
        WHEN trip_distance < 10 THEN '5-10 miles'
        WHEN trip_distance < 20 THEN '10-20 miles'
        ELSE '20+ miles'
    END AS distance_bucket,
    COUNT(*) AS total_rides,
    AVG(total_amount) AS avg_fare
FROM yellow_taxi_trips_september_comparison
WHERE trip_distance > 0
GROUP BY source_year, passenger_count, distance_bucket
ORDER BY source_year, passenger_count, distance_bucket;

-- 1️⃣6️⃣ Night vs. day rides revenue contribution
CREATE OR REPLACE VIEW rides_day_night_split_september AS
SELECT
    source_year,
    CASE
        WHEN HOUR(tpep_pickup_datetime) BETWEEN 6 AND 19 THEN 'Day'
        ELSE 'Night'
    END AS day_night,
    COUNT(*) AS total_rides,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM yellow_taxi_trips_september_comparison
GROUP BY source_year, day_night
ORDER BY source_year, day_night;
