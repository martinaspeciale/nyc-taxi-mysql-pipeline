# NYC Taxi MySQL Pipeline

An ETL pipeline to load NYC Yellow Taxi Trip data into a MySQL database for querying and analysis.

🚕 Data Source: [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Project Goals

- Demonstrate end-to-end data engineering pipeline.
- Handle modern data formats (Parquet).
- Load data into MySQL database.
- Perform advanced SQL queries and analysis.

## Project Structure

- `data/` → Raw Parquet files
- `converted_csv/` → CSV converted files
- `create_table.sql` → MySQL schema definition
- `load_data.py` → ETL script to convert + load data
- `README.md` → Project documentation

## Requirements

```bash
pandas
pyarrow
mysql-connector-python
```

## Usage 
 Coming soon 🚀 — in progress!