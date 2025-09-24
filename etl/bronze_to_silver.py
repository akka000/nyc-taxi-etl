import json, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from etl.logging_utils import get_logger

logger = get_logger()

BASE_DIR = \
os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONF_PATH= \
os.path.join(BASE_DIR, "config", "config.json")

with open(CONF_PATH) as f:
    cfg = json.load(f)

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

raw_parquet = os.path.join(BASE_DIR, cfg["local"]["raw_path"], \
                           "yellow_tripdata_2025-01.parquet")
silver_out = os.path.join(BASE_DIR, cfg["local"]["silver_path"], \
                                       "trips_silver.parquet")


logger.info("reading raw parquet %s", raw_parquet)

df = spark.read.parquet(raw_parquet)\
    .withColumn("pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))\
    .withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))\
    .withColumn("trip_duration_min",\
                (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime"))/60)\
    .withColumnRenamed("PULocationID", "pu_location_id")\
    .withColumnRenamed("DOLocationID", "do_location_id")\
    .filter((col("trip_duration_min") > 1) & (col("trip_duration_min") < 180)\
          & (col("passenger_count") > 0) & (col("fare_amount") > 0))

df.write.mode("overwrite").parquet(silver_out)

logger.info("silver written to %s", silver_out)

spark.stop()