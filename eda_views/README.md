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
