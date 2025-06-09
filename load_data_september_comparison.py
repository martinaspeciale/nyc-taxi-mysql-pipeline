import os
import glob
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv, find_dotenv
import argparse

# Argument parsing
parser = argparse.ArgumentParser(description='NYC Taxi Data Loader — September Comparison')
parser.add_argument('--insert-method', choices=['executemany', 'infile'], default='executemany',
                    help='Choose insert method: executemany (default) or infile (prints LOAD DATA INFILE command)')
args = parser.parse_args()

insert_method = args.insert_method
print(f"👉 Using insert method: {insert_method}")

# Load environment variables
load_dotenv(find_dotenv())

# MySQL config
MYSQL_CONFIG = {
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'host': os.getenv('MYSQL_HOST'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': int(os.getenv('MYSQL_PORT'))
}

TABLE_NAME = 'yellow_taxi_trips_september_comparison'

# Directories
PARQUET_DIR = './data/'
CSV_DIR = './converted_csv_september/'

# Ensure CSV dir exists
os.makedirs(CSV_DIR, exist_ok=True)

# Connect to MySQL
try:
    cnx = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = cnx.cursor()
    print("✅ Connected to MySQL.")
except mysql.connector.Error as err:
    print(f"❌ MySQL Error: {err}")
    exit(1)

# Process parquet files
parquet_files = glob.glob(os.path.join(PARQUET_DIR, 'september_comparison/yellow_tripdata_*.parquet'))

print(f"🔍 Found {len(parquet_files)} parquet file(s).")

infile_commands = []

for parquet_file in parquet_files:
    # Determine target CSV name
    csv_filename = os.path.join(CSV_DIR, os.path.basename(parquet_file).replace('.parquet', '_september.csv'))

    # If CSV already exists → skip this Parquet file
    if os.path.exists(csv_filename):
        print(f"⚠️ CSV already exists: {csv_filename} — skipping processing of {parquet_file}.")
        continue

    print(f"📄 Processing {parquet_file}...")

    # Load Parquet
    df = pd.read_parquet(parquet_file)

    # Force columns to lowercase
    df.columns = df.columns.str.lower()

    # Convert datetime columns safely
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

    # Filter only September rows
    df = df[df['tpep_pickup_datetime'].dt.month == 9]

    # If empty after filtering → skip
    if df.shape[0] == 0:
        print("⚠️ No September records found in this file. Skipping.")
        continue

    # Add source_year column
    df['source_year'] = df['tpep_pickup_datetime'].dt.year

    # Convert datetime back to string for MySQL insert
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Fill store_and_fwd_flag safely
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')

    # Columns to insert
    columns_to_insert = [
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'source_year'
    ]

    # Handle NaN → None
    df[columns_to_insert] = df[columns_to_insert].where(pd.notnull(df[columns_to_insert]), None)

    # Save CSV
    df.to_csv(csv_filename, index=False)
    print(f"💾 Saved CSV to {csv_filename}.")

    if insert_method == 'infile':
        infile_commands.append(f"""
            LOAD DATA LOCAL INFILE '{os.path.abspath(csv_filename)}'
            INTO TABLE {TABLE_NAME}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS;
            """)
        continue

    # Prepare values
    values = df[columns_to_insert].copy().values.tolist()

    print(f"Number of September records to insert: {len(values)}")

    # Prepare insert query
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge, source_year
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Batch insert
    batch_size = 1000
    for i in range(0, len(values), batch_size):
        batch = values[i:i+batch_size]
        try:
            cursor.executemany(insert_query, batch)
            cnx.commit()
            print(f"✅ Inserted batch {i} to {i+len(batch)-1}")
        except Exception as e:
            print(f"❌ Error inserting batch {i} to {i+len(batch)-1}: {e}")
            cnx.rollback()

if insert_method == 'infile':
    print("\n🚀 All LOAD DATA INFILE commands:")
    for cmd in infile_commands:
        print(cmd)

# Cleanup
cursor.close()
cnx.close()
print("🎉 Done.")
