import json
import os
import psycopg2
from pyspark.sql import SparkSession

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONF_PATH = os.path.join(BASE_DIR, "config", "config.json")

with open(CONF_PATH) as f:
    cfg = json.load(f)

spark = SparkSession.builder.appName("LoadToPostgres").getOrCreate()

silver_in = os.path.join(BASE_DIR, cfg["local"]["silver_path"], "trips_silver.parquet")
print(f"Reading silver data from: {silver_in}")

df = spark.read.parquet(silver_in)

df = df.repartition(8)  # adjust number based on memory and data size

db_config = {
    "dbname": "nyc_taxi",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": 5432
}

insert_query = """
INSERT INTO fact_trips(
    pickup_datetime, 
    dropoff_datetime, 
    pu_location_id, 
    do_location_id, 
    passenger_count, 
    trip_distance, 
    trip_duration_min, 
    fare_amount
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

def write_partition(partition):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    batch = []
    batch_size = 500  

    for r in partition:
        batch.append((
            r.pickup_datetime,
            r.dropoff_datetime,
            r.pu_location_id,
            r.do_location_id,
            r.passenger_count,
            r.trip_distance,
            r.trip_duration_min,
            r.fare_amount
        ))
        if len(batch) >= batch_size:
            cur.executemany(insert_query, batch)
            conn.commit()
            batch = []

    if batch:
        cur.executemany(insert_query, batch)
        conn.commit()

    cur.close()
    conn.close()

df.foreachPartition(write_partition)

spark.stop()
