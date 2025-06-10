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
