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
- `requirements.txt` → Project requirements


## Usage and Notes

### Running the Pipeline
#### 1️⃣ Batch insert (default mode)

- This mode inserts data into MySQL using executemany() in batches of 1000 rows.
- Command:

``` 
python3 load_data.py 
```

- What it does: Preprocesses all Parquet files (`yellow_tripdata_2024-*.parquet`)
- Saves preprocessed CSV files in `converted_csv/`
- Inserts data in batches into `yellow_taxi_trips` table

#### 2️⃣ Prepare CSVs for fast native loading (infile mode)

- This mode only preprocesses the data and saves clean CSV files.
- It does not insert the data — instead, it prints ready-to-use `LOAD DATA INFILE` SQL commands.
- Command:
``` 
python3 load_data.py --insert-method infile 
```
- What it does: Saves CSVs to `converted_csv/`
- Prints SQL commands to load each CSV into MySQL manually

### Using `LOAD DATA INFILE`
When using `--insert-method infile`, the pipeline prints `LOAD DATA INFILE` commands like:
```
LOAD DATA LOCAL INFILE '/full/path/to/converted_csv/yellow_tripdata_2024-01.csv'
INTO TABLE yellow_taxi_trips
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
```
Enabling `LOCAL INFILE`, which by default is disabled in MySQL for security reasons.

You must:
- Step 1: Enable it on the server:
```
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';
```
Expected result:
```
local_infile ON
```
- Step 2: Start the MySQL client with `LOCAL INFILE` enabled:
```
mysql --local-infile=1 -u martina -p -h 127.0.0.1 nyc_taxi
```
- Step 3: Run the printed `LOAD DATA INFILE` commands inside this client session.
If you skip these steps, you will see this error:
```
ERROR 3948 (42000): Loading local data is disabled; this must be enabled on both the client and server sides
```
#### Resetting the Table
Before loading the full dataset, it is strongly recommended to clear the target table to avoid duplicate rows:
```
TRUNCATE TABLE yellow_taxi_trips;
```
Pipeline Summary: 
* Step 1: Run `python3 load_data.py --insert-method infile` → saves CSVs, prints commands
* Step 2: `TRUNCATE TABLE yellow_taxi_trips`
* Step 3: Enable `LOCAL INFILE` and run printed `LOAD DATA INFILE` commands
* Step 4: Verify results:
```
SELECT COUNT(*) FROM yellow_taxi_trips;
```
#### Best Practices
- Use infile mode for faster bulk loading of large datasets
- Always verify `local_infile` is enabled before using `LOAD DATA INFILE`
- Use `TRUNCATE TABLE` before full reloads to avoid duplicate data
- Monitor CSV sizes and row counts to ensure complete processing
- Keep `.env` and `.gitignore` configured to avoid leaking sensitive data

## Next Steps Roadmap

The pipeline currently supports full ETL of the NYC Yellow Taxi data into MySQL.

Planned next steps:

- [ ] 1️⃣ Exploratory data analysis (EDA)
    - Total trips per month
    - Average fare and tip analysis
    - Payment types trends

- [ ] 2️⃣ Geospatial analysis 🌍
    - Top pickup and dropoff locations
    - Visualize trip density on a map
    - Temporal trends in pickup locations

- [ ] 3️⃣ Time series analysis
    - Hourly / daily activity patterns
    - Trends in trip distances and fares

- [ ] 4️⃣ Fares and tips deep dive
    - Average tip by payment type
    - Correlation between trip distance and tips

- [ ] 5️⃣ Outlier and data quality analysis
    - Detect anomalous trips (zero distance with high fare, extreme durations)

- [ ] 6️⃣ Build a small interactive dashboard 
    - Streamlit app or Jupyter notebook with charts and maps



