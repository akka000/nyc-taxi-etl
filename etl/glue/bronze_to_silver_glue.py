import json, os
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, unix_timestamp
import boto3


s3 = boto3.client("s3")
bucket = "akka000"
config_key = "dev/projects/nyc_taxi_etl/scripts/config/config.json"
obj = s3.get_object(Bucket=bucket, Key=config_key)
cfg = json.loads(obj["Body"].read())
raw_key = "dev/projects/nyc_taxi_etl/raw/yellow_tripdata_2025-01.parquet"
silver_key = "dev/projects/nyc_taxi_etl/silver/yellow_tripdata_2025-01.parquet" 


glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

df = spark.read.parquet(f"s3://{bucket}/{raw_key}")
df = df\
    .withColumn("pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))\
    .withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))\
    .withColumn("trip_duration_min", (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime"))/60)\
    .withColumnRenamed("PULocationID", "pu_location_id")\
    .withColumnRenamed("DOLocationID", "do_location_id")\
    .filter(
        (col("trip_duration_min") > 1)
        & (col("trip_duration_min") < 180)
        & (col("passenger_count") > 0)
        & (col("fare_amount") > 0)
    )

df.write.mode("overwrite").parquet(f"s3://{bucket}/{silver_key}")