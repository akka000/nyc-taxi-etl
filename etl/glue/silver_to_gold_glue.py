import json, os
from awsglue.context import GlueContext 
from pyspark.context import SparkContext 
from pyspark.sql.functions import to_date, col, count, avg, sum as sum_
import boto3
 

s3 = boto3.client("s3")
bucket = "akka000"
config_key = "dev/projects/nyc_taxi_etl/scripts/config/config.json"
obj = s3.get_object(Bucket=bucket, Key=config_key)
cfg = json.loads(obj["Body"].read())
silver_key = "dev/projects/nyc_taxi_etl/silver/yellow_tripdata_2025-01.parquet"
silver_in = f"s3://{bucket}/{silver_key}"
gold_out = f"s3://{bucket}/dev/projects/nyc_taxi_etl/gold/trips_gold.parquet" 

glueContext = GlueContext(SparkContext.getOrCreate()) 
spark = glueContext.spark_session 
 
df = spark.read.parquet(silver_in) 
 
df_agg = ( 
    df.withColumn("pickup_date", to_date(col("pickup_datetime"))) 
      .groupBy("pickup_date", "pu_location_id", "do_location_id") 
      .agg( 
          count("*").alias("total_trips"), 
          avg("passenger_count").alias("avg_passenger_count"), 
          avg("trip_distance").alias("avg_trip_distance"), 
          avg("trip_duration_min").alias("avg_duration_min"), 
          sum_("fare_amount").alias("total_fare"), 
          avg("fare_amount").alias("avg_fare") 
      ) 
) 
 
df_agg.write.mode("overwrite").parquet(gold_out)