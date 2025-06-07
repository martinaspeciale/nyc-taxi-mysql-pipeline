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

# Process only ONE test file (testing)
parquet_files = glob.glob(os.path.join(PARQUET_DIR, 'yellow_tripdata_2024-01.parquet'))

print(f"🔍 Found {len(parquet_files)} parquet file(s).")

for parquet_file in parquet_files:
    print(f"📄 Processing {parquet_file}...")
    
    # Load Parquet
    df = pd.read_parquet(parquet_file)

    # Force columns to lowercase
    df.columns = df.columns.str.lower()

    # Print for debug
    # print("Columns in DataFrame:", df.columns.tolist())

    # Convert Timestamp columns
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Define columns to insert
    columns_to_insert = [
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge'
    ]

    # Prepare values
    values = df[columns_to_insert].values.tolist()


    df.columns = [col.lower() for col in df.columns]

    # Convert Timestamp columns to string for MySQL
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')


    # Save to CSV (optional, for inspection)
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
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = df[[
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid',
        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge'
    ]].values.tolist()

    # print(f"Preparing to insert {len(values[0])} columns, expected {len(columns_to_insert)} columns.")
    for i, row in enumerate(values):
        try:
            cursor.execute(insert_query, row)
            cnx.commit()
            print(f"✅ Inserted row {i}")
        except Exception as e:
            print(f"❌ Error inserting row {i}: {e}")
            print("Row content:", row)
            break

'''
    try:
        cursor.executemany(insert_query, values)
        cnx.commit()
        print(f"✅ Loaded data from {csv_filename} into MySQL.")
    except Exception as e:
        print(f"❌ Error inserting data from {csv_filename}: {e}")
        cnx.rollback()
'''
# Cleanup
cursor.close()
cnx.close()
print("🎉 Done.")
