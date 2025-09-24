
import json, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg, sum as sum_
from etl.logging_utils import get_logger

logger = get_logger()


BASE_DIR = \
os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONF_PATH= \
os.path.join(BASE_DIR, "config", "config.json")

with open(CONF_PATH) as f:
    cfg = json.load(f)

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

silver_in = \
os.path.join(BASE_DIR, cfg["local"]["silver_path"], "trips_silver.parquet")

gold_out = \
os.path.join(BASE_DIR, cfg["local"]["gold_path"], "trips_gold.parquet")

logger.info("reading raw silver_in %s", silver_in)

df = spark.read.parquet(silver_in)

df_agg = \
df.withColumn("pickup_date", to_date(col("pickup_datetime"))).\
groupBy("pickup_date", "pu_location_id", "do_location_id").\
agg(count("*").alias("total_trips"), \
    avg("passenger_count").alias("avg_passenger_count"), \
    avg("trip_distance").alias("avg_trip_distance"), \
    avg("trip_duration_min").alias("avg_duration_min"), \
    sum_("fare_amount").alias("total_fare"), \
    avg("fare_amount").alias("avg_fare"))

df_agg.write.mode("overwrite").parquet(gold_out)

logger.info("gold written to %s", gold_out)

spark.stop()