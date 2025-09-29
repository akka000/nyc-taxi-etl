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

def choose_bronze_to_silver():
    return "bronze_to_silver_glue" if cfg["mode"] == "AWS" else "bronze_to_silver_local"

def choose_silver_to_gold():
    return "silver_to_gold_glue" if cfg["mode"] == "AWS" else "silver_to_gold_local"

def choose_dq():
    return "dq_glue" if cfg["mode"] == "AWS" else "dq_local"

def choose_sink(**kwargs):     
    if cfg["mode"] == "AWS": 
        return "redshift_copy" 
    else: 
        return "load_to_postgres" 
 

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

    branch_bronze_to_silver = BranchPythonOperator(
        task_id="branch_bronze_to_silver",
        python_callable=choose_bronze_to_silver,
    )

    bronze_to_silver_local = BashOperator(
        task_id="bronze_to_silver_local",
        bash_command=f"spark-submit {os.path.join(BASE_DIR, 'etl/bronze_to_silver.py')}",
    )

    bronze_to_silver_glue = AwsGlueJobOperator(
        task_id="bronze_to_silver_glue",
        job_name=cfg["aws"]["glue_job_name"]["bronze_to_silver"],
        script_location=(
            f"s3://{cfg['aws']['s3_bucket']}/dev/projects/nyc_taxi_etl/scripts/bronze_to_silver_glue.py"
        ),
        region_name=cfg["aws"]["region"],
    )

    branch_silver_to_gold = BranchPythonOperator(
        task_id="branch_silver_to_gold",
        python_callable=choose_silver_to_gold,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    silver_to_gold_local = BashOperator(
        task_id="silver_to_gold_local",
        bash_command=f"spark-submit {os.path.join(BASE_DIR, 'etl/silver_to_gold.py')}",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    silver_to_gold_glue = AwsGlueJobOperator(
        task_id="silver_to_gold_glue",
        job_name=cfg["aws"]["glue_job_name"]["silver_to_gold"],
        script_location=(
            f"s3://{cfg['aws']['s3_bucket']}/dev/projects/nyc_taxi_etl/scripts/silver_to_gold_glue.py"
        ),
        region_name=cfg["aws"]["region"],
    )

    branch_dq = BranchPythonOperator(
        task_id="branch_dq",
        python_callable=choose_dq,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    dq = BashOperator(
        task_id="dq",
        bash_command=f"spark-submit {os.path.join(BASE_DIR,'etl/data_quality.py')}",
    )

    dq_glue = AwsGlueJobOperator(
        task_id="dq_glue",
        job_name=cfg["aws"]["glue_job_name"]["dq"],
        script_location=(
            f"s3://{cfg['aws']['s3_bucket']}/dev/projects/nyc_taxi_etl/scripts/data_quality_glue.py"
        ),
        region_name=cfg["aws"]["region"],
    )

    branch_sink = BranchPythonOperator( 
        task_id="branch_sink", 
        python_callable=choose_sink, 
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    ) 

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"spark-submit {os.path.join(BASE_DIR,'etl/load_to_postgres.py')}",
    )

    redshift_copy = BashOperator(
        task_id="redshift_copy",
        bash_command=f"spark-submit {os.path.join(BASE_DIR,'etl/run_redshift_copy.py')}",
    )

    branch_bronze_to_silver >> [bronze_to_silver_local, bronze_to_silver_glue]
    [bronze_to_silver_local, bronze_to_silver_glue] >> branch_silver_to_gold
    branch_silver_to_gold >> [silver_to_gold_local, silver_to_gold_glue]
    [silver_to_gold_local, silver_to_gold_glue] >> branch_dq >> [dq, dq_glue]
    [dq, dq_glue] >> branch_sink >> [load_to_postgres, redshift_copy] 
    
