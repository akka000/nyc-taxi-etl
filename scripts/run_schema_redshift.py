import boto3, time

sql = open("warehouse/redshift_schema.sql").read() 
client = boto3.client("redshift-data", region_name="us-east-1") 
resp = client.execute_statement(ClusterIdentifier="nyc-taxi-dev-28-09-2025", Database="dev", 
DbUser="admin", Sql=sql) 
sid = resp['Id'] 
while True:
    d = client.describe_statement(Id=sid) 
    if d['Status'] in ('FINISHED','FAILED','ABORTED'): 
        print("Status:", d['Status']); break
    time.sleep(2) 
print("done") 