import os
import glob
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv, find_dotenv

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

TABLE_NAME = 'yellow_taxi_trips'

# Directories
PARQUET_DIR = './data/'
CSV_DIR = './converted_csv/'

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

# Process only ONE test file (for now)
parquet_files = glob.glob(os.path.join(PARQUET_DIR, 'yellow_tripdata_2024-01.parquet'))

print(f"🔍 Found {len(parquet_files)} parquet file(s).")

for parquet_file in parquet_files:
    print(f"📄 Processing {parquet_file}...")
    
    # Load Parquet
    df = pd.read_parquet(parquet_file)

    # Force columns to lowercase
    df.columns = df.columns.str.lower()

    # Convert datetime columns safely
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Columns to insert (must match table schema exactly)
    columns_to_insert = [
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge'
    ]

    # Handle NaN → None
    df[columns_to_insert] = df[columns_to_insert].where(pd.notnull(df[columns_to_insert]), None)

    # Fill store_and_fwd_flag safely (some rows may be missing)
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')

    # Save CSV (optional)
    csv_filename = os.path.join(CSV_DIR, os.path.basename(parquet_file).replace('.parquet', '.csv'))
    df.to_csv(csv_filename, index=False)
    print(f"💾 Saved CSV to {csv_filename}.")

    # Prepare values
    values = df[columns_to_insert].copy().values.tolist()

    print(f"Number of records to insert: {len(values)}")

    # Prepare insert query
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

# Cleanup
cursor.close()
cnx.close()
print("🎉 Done.")
