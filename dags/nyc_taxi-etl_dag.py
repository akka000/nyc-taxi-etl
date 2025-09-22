from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

BASE_DIR = \
os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

with DAG (
    dag_id="nyc_taxi_local",
    start_date=datetime(2025,1,1),
    schedule="@once",
    catchup=False,
    tags = ["etl_01"],
) as dag:
    
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"spark-submit \
        {os.path.join(BASE_DIR, 'etl/bronze_to_silver.py')}",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"spark-submit \
            {os.path.join(BASE_DIR, 'etl/silver_to_gold.py')}",
    )

    bronze_to_silver >> silver_to_gold
