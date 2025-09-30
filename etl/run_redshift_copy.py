import json, time, sys, boto3, os

BASE_DIR = \
os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONF_PATH= \
os.path.join(BASE_DIR, "config", "config.json")

with open(CONF_PATH) as f:
    cfg = json.load(f)

region = cfg["aws"]["region"] 
cluster_id = cfg["aws"]["redshift_cluster_id"] 
database = cfg["aws"]["redshift_database"] 
db_user = cfg["aws"]["redshift_db_user"] 
iam_role = cfg["aws"]["redshift_iam_role"] 
bucket = cfg["aws"]["s3_bucket"] 
s3_path = f"s3://{bucket}/dev/projects/nyc_taxi_etl/gold/trips_gold.parquet"

client = boto3.client("redshift-data", region_name=region) 
 
sql = f""" 
COPY trips_fact 
FROM '{s3_path}' 
IAM_ROLE '{iam_role}' 
FORMAT AS PARQUET; 
""" 
 
print("Submitting COPY to Redshift...") 
resp = client.execute_statement( 
    ClusterIdentifier=cluster_id, 
    Database=database, 
    DbUser=db_user, 
    Sql=sql 
) 
stmt_id = resp.get("Id") 
if not stmt_id: 
    print("Failed to start COPY", resp); sys.exit(1) 
 
while True: 
    desc = client.describe_statement(Id=stmt_id) 
    status = desc.get("Status") 
    print("Status:", status) 
    if status in ("FINISHED", "FAILED", "ABORTED"): 
        if status != "FINISHED": 
            print("COPY failed:", desc) 
            sys.exit(2) 
        print("COPY finished successfully.") 
        break 
    time.sleep(3)