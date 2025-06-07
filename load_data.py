import os
import glob
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

load_dotenv()

# Config
PARQUET_DIR = './data/'
CSV_DIR = './converted_csv/'
MYSQL_CONFIG = {
    'user': os.getenv('MYSQL_USER'),           
    'password': os.getenv('MYSQL_PASSWORD'),       
    'host': os.getenv('MYSQL_HOST'),
    'database': os.getenv('MYSQL_DATABASE'),       
    'port': os.getenv('MYSQL_PORT')
}

TABLE_NAME = 'yellow_taxi_trips'

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

# Process each Parquet file
parquet_files = glob.glob(os.path.join(PARQUET_DIR, '*.parquet'))
print(f"🔍 Found {len(parquet_files)} parquet files.")

for parquet_file in parquet_files:
    print(f"📄 Processing {parquet_file}...")
    
    # Load parquet
    df = pd.read_parquet(parquet_file)
    df.columns = [col.lower() for col in df.columns]

    # Save to CSV (optional but useful)
    csv_filename = os.path.join(CSV_DIR, os.path.basename(parquet_file).replace('.parquet', '.csv'))
    df.to_csv(csv_filename, index=False)
    print(f"💾 Saved CSV to {csv_filename}.")

    # Prepare data for insertion
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
        trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = df[[
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge'
    ]].values.tolist()

    try:
        cursor.executemany(insert_query, values)
        cnx.commit()
        print(f"✅ Loaded data from {csv_filename} into MySQL.")
    except Exception as e:
        print(f"❌ Error inserting data from {csv_filename}: {e}")
        cnx.rollback()

# Cleanup
cursor.close()
cnx.close()
print("🎉 Done.")
