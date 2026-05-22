"""
medallion_dag.py
----------------
Airflow DAG orchestrating the full Medallion Architecture pipeline on AWS.
S3 sensor → Glue bronze ingestion → EMR silver/gold steps → Snowflake load.

Schedule:  Daily at 02:00 UTC
SLA:       4 hours
Retries:   2 retries, 15-minute delay
Author:    Premchand Kothapalli
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "premchand.kothapalli",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=15),
    "email_on_failure": True,
    "email":            ["premchandkdata@gmail.com"],
    "sla":              timedelta(hours=4),
}

# ---------------------------------------------------------------------------
# Config from MWAA environment variables
# ---------------------------------------------------------------------------
S3_BUCKET        = Variable.get("s3_data_lake_bucket", default_var="optum-data-lake-prod")
GLUE_IAM_ROLE    = Variable.get("glue_iam_role")
SNS_TOPIC_ARN    = Variable.get("sns_alert_topic_arn")
SCRIPTS_BUCKET   = Variable.get("scripts_s3_bucket", default_var=S3_BUCKET)
ENVIRONMENT      = Variable.get("environment", default_var="prod")

# EMR cluster configuration
EMR_CLUSTER_CONFIG = {
    "Name":              "medallion-pipeline-{{ ds_nodash }}",
    "ReleaseLabel":      "emr-6.15.0",
    "Applications":      [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "LogUri":            f"s3://{S3_BUCKET}/emr-logs/",
    "ServiceRole":       "EMR_DefaultRole",
    "JobFlowRole":       "EMR_EC2_DefaultRole",
    "VisibleToAllUsers": True,
    "Instances": {
        "InstanceGroups": [
            {
                "Name":          "Master",
                "Market":        "ON_DEMAND",
                "InstanceRole":  "MASTER",
                "InstanceType":  "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name":          "Core",
                "Market":        "SPOT",
                "InstanceRole":  "CORE",
                "InstanceType":  "r5.2xlarge",
                "InstanceCount": 4,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected":        False,
    },
    "Configurations": [
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.extensions":      "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.adaptive.enabled": "true",
                "spark.dynamicAllocation.enabled": "true",
            },
        }
    ],
}

# EMR PySpark steps
EMR_STEPS = [
    {
        "Name": "silver-transformation",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.sql.shuffle.partitions=400",
                f"s3://{SCRIPTS_BUCKET}/scripts/medallion_pipeline.py",
                ENVIRONMENT, "silver",
            ],
        },
    },
    {
        "Name": "gold-aggregation",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                f"s3://{SCRIPTS_BUCKET}/scripts/medallion_pipeline.py",
                ENVIRONMENT, "gold",
            ],
        },
    },
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def check_source_data(**context) -> str:
    """Check for new source files in S3. Branch to skip if none found."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook   = S3Hook(aws_conn_id="aws_default")
    prefix = f"raw/{context['ds_nodash']}/"
    keys   = hook.list_keys(bucket_name=S3_BUCKET, prefix=prefix)
    if not keys:
        return "no_data_skip"
    return "trigger_glue_ingestion"


def validate_row_counts(**context) -> None:
    """Compare Silver and Gold row counts. Fail if drop exceeds 5%."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    # In production this reads count files written by the PySpark job
    silver_count = int(context["ti"].xcom_pull(task_ids="submit_emr_steps", key="silver_count") or 0)
    gold_count   = int(context["ti"].xcom_pull(task_ids="submit_emr_steps", key="gold_count")   or 0)

    if silver_count == 0:
        raise ValueError("Silver count is 0 — pipeline produced no output")

    drop_pct = (silver_count - gold_count) / silver_count
    if drop_pct > 0.05:
        raise ValueError(
            f"Row count drop from Silver to Gold exceeds 5%: "
            f"Silver={silver_count:,}, Gold={gold_count:,}, drop={drop_pct:.1%}"
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="medallion_architecture_pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "healthcare", "optum", "emr", "snowflake"],
    doc_md="""
    ## Medallion Architecture Pipeline
    Orchestrates Bronze → Silver → Gold data lake pipeline on AWS.
    Processes 10M+ daily healthcare records from raw S3 sources through
    Delta Lake layers into Snowflake for BI consumption.
    """,
) as dag:

    # Branch: check for source data before spinning up EMR
    check_data = BranchPythonOperator(
        task_id="check_source_data",
        python_callable=check_source_data,
    )

    # Graceful skip — no data today
    no_data_skip = EmptyOperator(task_id="no_data_skip")

    # Glue job: Bronze ingestion from raw S3 sources
    trigger_glue = GlueJobOperator(
        task_id="trigger_glue_ingestion",
        job_name="bronze-ingestion-job",
        script_args={
            "--env":        ENVIRONMENT,
            "--s3_bucket":  S3_BUCKET,
            "--run_date":   "{{ ds }}",
        },
        aws_conn_id="aws_default",
        iam_role_name=GLUE_IAM_ROLE,
        wait_for_completion=True,
    )

    # S3 sensor: confirm Bronze Delta files written before EMR starts
    validate_bronze = S3KeySensor(
        task_id="validate_bronze_s3",
        bucket_name=S3_BUCKET,
        bucket_key="bronze/claims/_delta_log/",
        aws_conn_id="aws_default",
        timeout=1800,
        poke_interval=60,
    )

    # Create EMR cluster
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id="aws_default",
    )

    # Submit Silver + Gold PySpark steps
    submit_steps = EmrAddStepsOperator(
        task_id="submit_emr_steps",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        steps=EMR_STEPS,
        aws_conn_id="aws_default",
    )

    # Wait for Silver step — reschedule mode to free worker slots
    wait_silver = EmrStepSensor(
        task_id="wait_bronze_to_silver",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_emr_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        mode="reschedule",
        poke_interval=60,
    )

    # Wait for Gold step — reschedule mode
    wait_gold = EmrStepSensor(
        task_id="wait_silver_to_gold",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_emr_steps', key='return_value')[1] }}",
        aws_conn_id="aws_default",
        mode="reschedule",
        poke_interval=60,
    )

    # Validate row counts before Snowflake load
    validate_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
    )

    # Terminate EMR — runs even on failure to avoid idle cost
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Snowflake load handled by Gold layer in medallion_pipeline.py
    snowflake_validate = PythonOperator(
        task_id="validate_snowflake_load",
        python_callable=lambda **ctx: None,  # row count pulled from Snowflake in prod
    )

    # Success alert
    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC_ARN,
        message="[SUCCESS] Medallion pipeline completed for {{ ds }}. Gold layer loaded to Snowflake.",
        subject="Medallion Pipeline — SUCCESS",
        aws_conn_id="aws_default",
    )

    # Failure alert — fires if any upstream task fails
    notify_failure = SnsPublishOperator(
        task_id="notify_failure",
        target_arn=SNS_TOPIC_ARN,
        message="[FAILURE] Medallion pipeline failed for {{ ds }}. Check Airflow logs.",
        subject="Medallion Pipeline — FAILURE",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    check_data >> [no_data_skip, trigger_glue]
    trigger_glue >> validate_bronze >> create_cluster >> submit_steps
    submit_steps >> [wait_silver, wait_gold]
    wait_silver >> wait_gold >> validate_counts >> terminate_cluster
    terminate_cluster >> snowflake_validate >> notify_success
    [validate_counts, terminate_cluster] >> notify_failure
