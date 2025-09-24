import json, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE_DIR = \
os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONF_PATH= \
os.path.join(BASE_DIR, "config", "config.json")

with open(CONF_PATH) as f:
    cfg = json.load(f)


gold_in = \
os.path.join(BASE_DIR, cfg["local"]["gold_path"], "trips_gold.parquet")

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

df = spark.read.parquet(gold_in)

errors = []
if df.filter(col("avg_fare") <= 0).count() > 0:
    errors.append("avg_fare <= 0 present")
if df.filter(col("avg_passenger_count") <= 0).count() > 0:
    errors.append("avg_passenger_count <= 0 present")
if df.filter(
    (col("avg_duration_min") < 1) |
    (col("avg_duration_min") > 180)).count() > 0:
    errors.append("avg_duration_min out of range")

if errors:
    print("||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
    raise Exception("DQ Failed:" + ";".join(errors))

else:
    print("||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
    print("DQ Passed")
    print("||||||||||||||||||||||||||||||||||||||||||||||||||||||||")

spark.stop()