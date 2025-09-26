from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
#from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator as AwsGlueJobOperator
from datetime import datetime, timedelta
import os, json
from airflow.utils.trigger_rule import TriggerRule

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONF_PATH = os.path.join(BASE_DIR, "config", "config.json")

with open(CONF_PATH) as f:
    cfg = json.load(f)

def choose_bronze():
    return "bronze_to_silver_glue" if cfg["mode"] == "AWS" else "bronze_to_silver_local"

default_args = {
    "depends_on_past": False,
}

with DAG(
    dag_id="nyc_taxi_full_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["etl"],
) as dag:

    choose_task = BranchPythonOperator(
        task_id="choose_bronze_path",
        python_callable=choose_bronze,
    )

    bronze_to_silver_local = BashOperator(
        task_id="bronze_to_silver_local",
        bash_command=f"spark-submit {os.path.join(BASE_DIR, 'etl/bronze_to_silver.py')}",
    )

    bronze_to_silver_glue = AwsGlueJobOperator(
        task_id="bronze_to_silver_glue",
        job_name=cfg["aws"]["glue_job_name"],
        script_location=(
            f"s3://{cfg['aws']['s3_bucket']}/dev/projects/nyc_taxi_etl/scripts/bronze_to_silver_glue.py"
        ),
        region_name=cfg["aws"]["region"],
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"spark-submit {os.path.join(BASE_DIR, 'etl/silver_to_gold.py')}",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    dq = BashOperator(
        task_id="data_quality",
        bash_command=f"spark-submit {os.path.join(BASE_DIR,'etl/data_quality.py')}",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"spark-submit {os.path.join(BASE_DIR,'etl/load_to_postgres.py')}",
    )

    choose_task >> [bronze_to_silver_local, bronze_to_silver_glue]
    [bronze_to_silver_local, bronze_to_silver_glue] >> silver_to_gold >> dq >> load_to_postgres
