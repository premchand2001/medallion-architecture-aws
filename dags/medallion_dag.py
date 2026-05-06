"""
Airflow DAG — Medallion Architecture Orchestration
Amazon MWAA | Bronze → Silver → Gold Pipeline
Author: Premchand Kothapalli
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import logging

logger = logging.getLogger(__name__)

# ─── Default Args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@yourcompany.com"],
    "sla": timedelta(hours=4),
}

# ─── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="medallion_architecture_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold Medallion Architecture on AWS S3 + Delta Lake",
    schedule_interval="0 6 * * *",  # Daily at 6AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "delta-lake", "healthcare", "aws"],
) as dag:

    # ── Sensor: wait for raw data to land in S3 ────────────────────────────────
    wait_for_raw_data = S3KeySensor(
        task_id="wait_for_raw_data",
        bucket_name="your-data-lake-bucket",
        bucket_key="raw/claims/{{ ds_nodash }}/*.csv",
        aws_conn_id="aws_default",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # ── BRONZE: Ingest raw data via Glue ──────────────────────────────────────
    bronze_ingestion = GlueJobOperator(
        task_id="bronze_ingestion",
        job_name="bronze-raw-ingestion",
        script_args={
            "--domain": "claims",
            "--source_path": "s3://your-data-lake-bucket/raw/claims/{{ ds_nodash }}/",
            "--target_path": "s3://your-data-lake-bucket/bronze/claims/",
            "--run_date": "{{ ds }}",
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        num_of_dpus=5,
    )

    # ── SILVER: Clean + validate via EMR PySpark ──────────────────────────────
    silver_steps = [
        {
            "Name": "Silver Transformation",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                    "s3://your-scripts-bucket/medallion_pipeline.py",
                    "--layer", "silver",
                    "--domain", "claims",
                    "--run_date", "{{ ds }}",
                ],
            },
        }
    ]

    add_silver_step = EmrAddStepsOperator(
        task_id="add_silver_step",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=silver_steps,
        aws_conn_id="aws_default",
    )

    silver_sensor = EmrStepSensor(
        task_id="silver_sensor",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull('add_silver_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=60,
    )

    # ── GOLD: Build star-schema + load Snowflake ──────────────────────────────
    gold_steps = [
        {
            "Name": "Gold Aggregation",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "s3://your-scripts-bucket/medallion_pipeline.py",
                    "--layer", "gold",
                    "--domain", "claims",
                    "--run_date", "{{ ds }}",
                ],
            },
        }
    ]

    add_gold_step = EmrAddStepsOperator(
        task_id="add_gold_step",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=gold_steps,
        aws_conn_id="aws_default",
    )

    gold_sensor = EmrStepSensor(
        task_id="gold_sensor",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull('add_gold_step')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=60,
    )

    # ── Snowflake: Run post-load validation ───────────────────────────────────
    validate_snowflake = SnowflakeOperator(
        task_id="validate_snowflake_load",
        sql="""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT claim_id) AS unique_claims,
                MIN(_gold_timestamp) AS earliest_load,
                MAX(_gold_timestamp) AS latest_load
            FROM HEALTHCARE_DW.GOLD.CLAIMS_FACT
            WHERE DATE(service_date) = '{{ ds }}';
        """,
        snowflake_conn_id="snowflake_default",
    )

    # ── Pipeline dependency chain ─────────────────────────────────────────────
    wait_for_raw_data >> bronze_ingestion >> add_silver_step >> silver_sensor
    silver_sensor >> add_gold_step >> gold_sensor >> validate_snowflake
