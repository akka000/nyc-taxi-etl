import json, os
from awsglue.context import GlueContext 
from pyspark.context import SparkContext 
from pyspark.sql.functions import col
import boto3


s3 = boto3.client("s3")
bucket = "akka000"
config_key = "dev/projects/nyc_taxi_etl/scripts/config/config.json"
obj = s3.get_object(Bucket=bucket, Key=config_key)
cfg = json.loads(obj["Body"].read())

gold_in = f"s3://{bucket}/dev/projects/nyc_taxi_etl/gold/trips_gold.parquet"

glueContext = GlueContext(SparkContext.getOrCreate()) 
spark = glueContext.spark_session

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