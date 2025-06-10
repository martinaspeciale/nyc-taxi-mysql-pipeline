# EDA Views for NYC Taxi Pipeline

This folder contains reusable **SQL views** used in:

- Tableau dashboards
- Python EDA scripts (`eda_results/`)
- Streamlit dashboard

The views are defined in [`views.sql`](views.sql).  
They are built on top of the `yellow_taxi_trips` table and provide a clean semantic layer.

---

## How to update views

You can apply the views to your MySQL database by running:

```bash
cd eda_views
chmod +x run_views.sh     # make script executable (only needed once)
./run_views.sh            # run the script to update all views

### EDA Views — One-Month Comparison Support

####  Note: Maintaining source_year column

The `yellow_taxi_trips_september_comparison` table includes a `source_year` column to support multi-year analysis.

If data is loaded manually (inline INSERTs or LOAD DATA INFILE) without including the `source_year`, run the following command to populate it:

```sql
UPDATE yellow_taxi_trips_september_comparison
SET source_year = YEAR(tpep_pickup_datetime)
WHERE source_year IS NULL;
```
### Use batch updates instead of a single full-table update

Initially, we tried to update the `source_year` column for all rows in the `yellow_taxi_trips_september_comparison` table with a single large query (as shown above).
However, this caused the database server to crash or become unresponsive due to the large size of the table (millions of rows). Performing the full update in one operation required too much memory and caused long-running locks on the table, which is not suitable for production environments.

To address this, we switched to a batch update strategy:
```sql
UPDATE yellow_taxi_trips_september_comparison
SET source_year = YEAR(tpep_pickup_datetime)
WHERE source_year IS NULL
  AND YEAR(tpep_pickup_datetime) IS NOT NULL
  AND (source_year <> YEAR(tpep_pickup_datetime) OR source_year IS NULL)
LIMIT 10000;
```

---

The following EDA views can also be used with the `yellow_taxi_trips_september_comparison` table to support **multi-year September analysis**:

- `rides_per_day`
- `rides_per_hour_of_day`
- `rides_by_passenger_count`
- `fare_vs_distance`
- `tip_percentage_per_week` (adapted to per-day or per-month for September)
- `passenger_count_trip_distance_grid`
- `pickup_hour x dropoff_hour matrix`
- `rides_day_night_split`
- and other behavior analysis views

**Purpose:** enable creation of a dedicated **"How NYC moves in September across years"** dashboard in Tableau, supporting:

- Cross-year comparison of urban mobility patterns
- Visualization of seasonal trends
- Detection of year-over-year anomalies or evolution
- Storytelling visualizations (heatmaps, race charts, temporal animations)

➡️ These views can be re-used by simply switching the table reference in the SQL or Tableau data source.

➡️ The `yellow_taxi_trips_september_comparison` table includes the `source_year` column to support easy year-based faceting and trend analysis.
