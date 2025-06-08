# Notebooks — NYC Yellow Taxi MySQL Pipeline

This directory contains exploratory notebooks built on top of the ETL pipeline for NYC Yellow Taxi Trip Data.

## Available notebooks

- `eda_yellow_taxi.ipynb` → Exploratory data analysis (EDA): total trips, fare and tip analysis, payment trends
- `geospatial_analysis.ipynb` → Exploratory geospatial analysis of pickup zones

## Data Schema Reference

The main table used in analysis is `yellow_taxi_trips`, with the following columns:

```
| Column                  | Type        | Description                           |
|-------------------------|-------------|---------------------------------------|
| vendor_id               | varchar(10) | Vendor identifier                     |
| tpep_pickup_datetime    | datetime    | Pickup timestamp                      |
| tpep_dropoff_datetime   | datetime    | Dropoff timestamp                     |
| passenger_count         | int         | Number of passengers                  |
| trip_distance           | float       | Trip distance in miles                |
| rate_code_id            | int         | Rate code used                        |
| store_and_fwd_flag      | char(1)     | Whether trip was stored and forwarded |
| pu_location_id          | int         | Pickup location ID                    |
| do_location_id          | int         | Dropoff location ID                   |
| payment_type            | int         | Payment type code                     |
| fare_amount             | float       | Fare amount in USD                    |
| extra                   | float       | Extra charges                         |
| mta_tax                 | float       | MTA tax                               |
| tip_amount              | float       | Tip amount in USD                     |
| tolls_amount            | float       | Tolls amount in USD                   |
| improvement_surcharge   | float       | Improvement surcharge                 |
| total_amount            | float       | Total amount charged                  |
| congestion_surcharge    | float       | Congestion surcharge                  |
```

## Notes

- The notebooks are designed to work on a MySQL database containing the full 2024 NYC Yellow Taxi dataset (~41M records).
- Indexes are created automatically where needed to optimize query performance.

