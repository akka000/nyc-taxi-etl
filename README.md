# NYC Taxi ETL with Airflow (Local & AWS)

This project demonstrates an end-to-end ETL pipeline using Airflow, Glue, S3, and 
Redshift. 
The pipeline ingests raw taxi trip data, transforms it through bronze → silver → gold layers, 
validates with data quality checks, and finally loads results into Redshift or Postgres 
depending on mode. 
- **Local Mode**: All ETL steps run in Python locally with Airflow. 
- **AWS Mode**: Transformations run as Glue Jobs on S3 data, and final results are 
loaded into Amazon Redshift.

### Steps: 
1. **Bronze (Raw Layer)**
   - Extracts raw CSV/Parquet files (e.g., yellow_tripdata_2022-01.parquet). 
   - Stores them as-is in local storage (./data/raw/) or S3 
(s3://bucket/raw/). 
2. **Silver (Cleaned & Structured Layer)**
   - Reads bronze data. 
   - Cleans nulls, fixes schema, standardizes datetime and numeric fields. 
   - Outputs partitioned parquet files in data/silver/ or 
s3://bucket/silver/. 
3. **Gold (Aggregated / Analytics Layer)**
   - Reads silver data. 
   - Produces curated dataset (fact table) with fields like: 
      - vendorid, 
      - pickup/dropoff datetime, 
      - passenger_count, 
      - trip_distance, 
      - payment_type, 
      - fare_amount. 
      - Outputs as parquet (single file in AWS mode, directory locally). 
4. **Data Quality Checks (DQ)**
   - Ensures no critical columns are null. 
   - Validates row counts. 
   - Optionally checks min/max ranges (e.g., trip_distance > 0). 
5. **Load (Sink)**
   - Local mode: loads gold dataset into PostgreSQL for queries. 
   - AWS mode: copies gold dataset from S3 into Amazon Redshift fact table.

### Final Output 
A cleaned fact table of NYC taxi trips that can be queried for: 
- Total trips per day / hour. 
- Average fare per passenger count. 
- Distribution of trip distances. 
- Payment type trends (cash vs card).

## Prerequisites 
**Local Tools** 
- Python 3.11 
- Airflow
- AWS CLI
- Boto3
- PySpark
- PostgreSQL 

**AWS Resources** 
- S3 Bucket (e.g., nyc-taxi-bucket) 
- AWS Glue
- Amazon Redshift (Provisioned) cluster 
- IAM Role with policies: 
   - AmazonS3FullAccess 
   - AWSGlueServiceRole 
   - AmazonRedshiftFullAccess

## Setup Instructions

### A. Local Mode Setup

Install dependencies:
 
    pip install apache-airflow    
    pip install pyspark
    pip install psycopg2-binary
    pip install boto3
    sudo apt update
    sudo apt install postgresql postgresql-contrib -y
 
  
Initialize Airflow:
 
    airflow db init 
    airflow users create \
    --username admin --firstname Arman --lastname Khaxar \
    --role Admin --email admin@example.com 
  
Start services:
 
    airflow standalone 
  
Trigger DAG:
 
    airflow dags trigger nyc_taxi_etl 

### B. AWS Mode Setup

**Step 1. Create an S3 Bucket**
 
    aws s3 mb s3://akka000 --region us-east-1 
Upload Glue job scripts: 

    aws s3 cp etl/glue/ \
    s3://akka000/dev/projects/nyc_taxi_etl/scripts/ --recursive 
**Step 2. IAM Role for Glue and Redshift**
 
1. In IAM Console, create a role named GlueRedshiftRole. 
2. Attach policies: 
   - AmazonS3FullAccess 
   - AWSGlueServiceRole 
   - AmazonRedshiftFullAccess 
3. Copy its ARN into config.json under redshift_iam_role.

**Step 3. Create Glue Jobs**
 
For each Glue job:
 
1. Open AWS Glue Console → Jobs → Add job. 
2. Name: 
   - bronze-to-silver 
   - silver-to-gold 
   - dq 
3. IAM Role: GlueRedshiftRole 
4. Script Path: s3://akka000/dev/projects/nyc_taxi_etl/scripts/<job_file>.py 
5. Type: Spark 
6. Glue Version: 5.0 
7. Connections: none needed if only S3. 
8. Save.
 
**Step 4. Provision Redshift Cluster**
 
1. Go to Amazon Redshift → Clusters → Create cluster. 
2. Choose Provisioned. 
   - Cluster Identifier: nyc-taxi-dev-25-08-2025 
   - Node type: ra3.large (single-node) 
   - Database name: dev 
   - Master user: admin 
   - Password: (set a secure password) 
3. Wait until cluster is available.
 
**Step 5. Configure Redshift Security Group**
1. In EC2 Console → Security Groups, find the SG used by your cluster. 
2. Edit Inbound Rules: 
   - Type: All traffic (or PostgreSQL)
   - Protocol: All (or TCP)
   - Port: 5439 
   - Source: My IP
3. Save rules. 
 
**Step 6. Create Table in Redshift**
 
Run with AWS CLI:
 
    aws redshift-data execute-statement \ 
      --region us-east-1 \ 
      --cluster-identifier nyc-taxi-dev-28-08-2025 \ 
      --database dev \ 
      --db-user admin \ 
      --sql " 
    CREATE TABLE IF NOT EXISTS trips_fact ( 
        vendorid INT, 
        tpep_pickup_datetime TIMESTAMP, 
        tpep_dropoff_datetime TIMESTAMP, 
        passenger_count INT, 
        trip_distance FLOAT, 
        payment_type INT, 
        fare_amount FLOAT 
    ); 
    " 
 
 
**Step 7. Run DAG in AWS Mode**

1. Set "mode": "AWS" in config/config.json. 
2. Start Airflow services.
 
Trigger DAG:
 
    airflow dags trigger nyc_taxi_etl

3.This will: 
- Upload raw → bronze in S3. 
- Run Glue jobs to transform bronze → silver → gold. 
- Run Glue job for DQ. 
- Convert gold output to single parquet. 
- COPY into Redshift.

## DAG Flow 
Local Mode:
 
extract_bronze → bronze_to_silver_local → silver_to_gold_local → dq_local → load_to_postgres
 
AWS Mode:
 
extract_bronze → bronze_to_silver_glue → silver_to_gold_glue → dq_glue → redshift_copy  