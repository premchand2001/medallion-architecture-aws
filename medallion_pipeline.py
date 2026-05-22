"""
cdc_incremental_ingestion.py
----------------------------
CDC-based incremental ingestion pipeline for Amazon MWAA.
AWS DMS health check → schema drift detection → Glue CDC processing
→ DQ gate → UPSERT merge into Redshift production tables.

Schedule:  Every 4 hours
Retries:   3 retries, 10-minute delay
Author:    Premchand Kothapalli
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "premchand.kothapalli",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          3,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": True,
    "email":            ["premchandkdata@gmail.com"],
}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
S3_BUCKET              = Variable.get("s3_data_lake_bucket",       default_var="att-cdc-landing-prod")
SNS_TOPIC_ARN          = Variable.get("sns_alert_topic_arn")
DMS_TASK_ARN           = Variable.get("dms_replication_task_arn")
GLUE_IAM_ROLE          = Variable.get("glue_iam_role")
ENVIRONMENT            = Variable.get("environment",               default_var="prod")
REDSHIFT_CONN_ID       = "redshift_prod"
CDC_TABLES             = ["customer_events", "customer_profile", "subscription_changes"]


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------
def check_dms_task_health(**context) -> str:
    """Verify DMS replication task is running before processing CDC files."""
    import boto3
    client = boto3.client("dms")

    response = client.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [DMS_TASK_ARN]}]
    )
    tasks = response.get("ReplicationTasks", [])
    if not tasks:
        raise ValueError(f"DMS task not found: {DMS_TASK_ARN}")

    status = tasks[0]["Status"]
    if status != "running":
        context["ti"].xcom_push(key="dms_status", value=status)
        return "dms_not_running_alert"

    log_latency = tasks[0].get("ReplicationTaskStats", {}).get("CDCLatencySource", "N/A")
    context["ti"].xcom_push(key="cdc_latency", value=str(log_latency))
    return "detect_schema_drift"


def detect_schema_drift(**context) -> str:
    """
    Compare Glue Catalog schema against expected schema definition.
    Halt if any column was added, dropped, or renamed.
    """
    import boto3
    import json

    glue = boto3.client("glue")

    # Expected schemas loaded from S3 config file in prod
    expected_schemas = {
        "customer_events":      ["customer_id", "event_id", "event_type", "event_timestamp", "metadata"],
        "customer_profile":     ["customer_id", "segment", "risk_tier", "created_at", "updated_at"],
        "subscription_changes": ["customer_id", "subscription_id", "change_type", "effective_date"],
    }

    drift_detected = []
    for table_name in CDC_TABLES:
        try:
            response = glue.get_table(DatabaseName="att_catalog", Name=table_name)
            actual_cols = {
                col["Name"]
                for col in response["Table"]["StorageDescriptor"]["Columns"]
            }
            expected_cols = set(expected_schemas.get(table_name, []))
            added   = actual_cols   - expected_cols
            removed = expected_cols - actual_cols

            if added or removed:
                drift_detected.append({
                    "table":   table_name,
                    "added":   list(added),
                    "removed": list(removed),
                })
        except Exception as e:
            context["ti"].log.warning(f"Could not check schema for {table_name}: {e}")

    if drift_detected:
        context["ti"].xcom_push(key="drift_details", value=json.dumps(drift_detected))
        return "schema_drift_alert"

    return "trigger_glue_cdc_processing"


def run_dq_checks(**context) -> None:
    """
    Data quality gate — runs before merge step.
    Fails pipeline if any check exceeds threshold.
    Checks: row count delta, null rates on critical columns, duplicate primary keys.
    """
    from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

    hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)

    dq_config = {
        "customer_events": {
            "key_cols":       ["customer_id", "event_id"],
            "null_check_cols": ["customer_id", "event_type", "event_timestamp"],
            "null_threshold":  0.05,
        },
        "customer_profile": {
            "key_cols":       ["customer_id"],
            "null_check_cols": ["customer_id", "segment"],
            "null_threshold":  0.02,
        },
    }

    for table_name, config in dq_config.items():
        staging_table = f"{table_name}_staging"

        # Row count — staging must have rows
        count = hook.get_first(f"SELECT COUNT(*) FROM staging.{staging_table}")[0]
        if count == 0:
            raise ValueError(f"[DQ FAIL] {staging_table}: 0 rows in staging — CDC may have stalled")

        # Null check on critical columns
        total = count
        for col in config["null_check_cols"]:
            null_count = hook.get_first(
                f"SELECT COUNT(*) FROM staging.{staging_table} WHERE {col} IS NULL"
            )[0]
            null_pct = null_count / total if total > 0 else 0
            if null_pct > config["null_threshold"]:
                raise ValueError(
                    f"[DQ FAIL] {staging_table}.{col}: {null_pct:.1%} nulls "
                    f"exceeds threshold {config['null_threshold']:.0%}"
                )

        # Duplicate key check
        key_cols_sql = ", ".join(config["key_cols"])
        dupe_count = hook.get_first(f"""
            SELECT COUNT(*) FROM (
                SELECT {key_cols_sql}, COUNT(*) AS cnt
                FROM staging.{staging_table}
                GROUP BY {key_cols_sql}
                HAVING COUNT(*) > 1
            ) d
        """)[0]

        if dupe_count > 0:
            raise ValueError(
                f"[DQ FAIL] {staging_table}: {dupe_count:,} duplicate keys on ({key_cols_sql})"
            )

    context["ti"].log.info("[DQ] All checks passed ✓")


def merge_staging_to_production(**context) -> dict:
    """
    UPSERT via DELETE + INSERT — efficient on Redshift's columnar storage.
    Avoids row-level UPDATE that fragments columnar blocks.
    """
    from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

    hook    = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    results = {}

    merge_configs = {
        "customer_events":      ["customer_id", "event_id"],
        "customer_profile":     ["customer_id"],
        "subscription_changes": ["customer_id", "subscription_id"],
    }

    for table_name, key_cols in merge_configs.items():
        staging_table = f"{table_name}_staging"
        key_join_cond = " AND ".join(
            [f"prod.{k} = stg.{k}" for k in key_cols]
        )

        # Step 1: Delete matching keys from production
        hook.run(f"""
            DELETE FROM production.{table_name} prod
            USING staging.{staging_table} stg
            WHERE {key_join_cond}
        """)

        # Step 2: Insert all staging rows into production
        hook.run(f"""
            INSERT INTO production.{table_name}
            SELECT * FROM staging.{staging_table}
        """)

        # Get final count for reporting
        final_count = hook.get_first(f"SELECT COUNT(*) FROM production.{table_name}")[0]
        results[table_name] = final_count
        context["ti"].log.info(f"[MERGE] {table_name}: {final_count:,} rows in production ✓")

    context["ti"].xcom_push(key="merge_results", value=str(results))
    return results


def publish_dq_summary(**context) -> None:
    """Log DQ summary and push metrics for downstream monitoring."""
    merge_results = context["ti"].xcom_pull(task_ids="merge_staging_to_production",
                                             key="merge_results")
    cdc_latency   = context["ti"].xcom_pull(task_ids="check_dms_task_health",
                                             key="cdc_latency")
    context["ti"].log.info(f"CDC latency: {cdc_latency}s")
    context["ti"].log.info(f"Merge results: {merge_results}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="cdc_incremental_ingestion",
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "dms", "redshift", "att", "hgs"],
    doc_md="""
    ## CDC Incremental Ingestion Pipeline
    Captures row-level changes from source systems via AWS DMS and
    merges them into production Redshift tables every 4 hours.
    DMS health check → schema drift detection → DQ gate → UPSERT merge.
    """,
) as dag:

    # Branch: DMS health check
    dms_health_check = BranchPythonOperator(
        task_id="check_dms_task_health",
        python_callable=check_dms_task_health,
    )

    # DMS not running — alert and stop
    dms_not_running = SnsPublishOperator(
        task_id="dms_not_running_alert",
        target_arn=SNS_TOPIC_ARN,
        message="[ERROR] DMS replication task is not running. CDC pipeline halted.",
        subject="CDC Pipeline — DMS Task Not Running",
        aws_conn_id="aws_default",
    )

    # Branch: schema drift detection
    schema_drift_check = BranchPythonOperator(
        task_id="detect_schema_drift",
        python_callable=detect_schema_drift,
    )

    # Schema drift detected — alert and stop
    drift_alert = SnsPublishOperator(
        task_id="schema_drift_alert",
        target_arn=SNS_TOPIC_ARN,
        message="[ERROR] Schema drift detected. CDC pipeline halted pending schema review.",
        subject="CDC Pipeline — Schema Drift Detected",
        aws_conn_id="aws_default",
    )

    # Glue job: process CDC files from S3 landing zone into staging tables
    glue_cdc = GlueJobOperator(
        task_id="trigger_glue_cdc_processing",
        job_name="cdc-incremental-glue-job",
        script_args={
            "--env":         ENVIRONMENT,
            "--s3_bucket":   S3_BUCKET,
            "--tables":      ",".join(CDC_TABLES),
            "--run_date":    "{{ ds }}",
            "--run_hour":    "{{ execution_date.hour }}",
        },
        aws_conn_id="aws_default",
        iam_role_name=GLUE_IAM_ROLE,
        wait_for_completion=True,
    )

    # DQ gate — must pass before merge
    dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_dq_checks,
    )

    # UPSERT: DELETE + INSERT into production
    merge = PythonOperator(
        task_id="merge_staging_to_production",
        python_callable=merge_staging_to_production,
    )

    # Publish DQ summary metrics
    dq_summary = PythonOperator(
        task_id="publish_dq_summary",
        python_callable=publish_dq_summary,
    )

    # Success alert
    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC_ARN,
        message="[SUCCESS] CDC incremental ingestion complete for {{ execution_date }}.",
        subject="CDC Pipeline — SUCCESS",
        aws_conn_id="aws_default",
    )

    # Failure alert — fires on any upstream failure
    notify_failure = SnsPublishOperator(
        task_id="notify_failure",
        target_arn=SNS_TOPIC_ARN,
        message="[FAILURE] CDC pipeline failed for {{ execution_date }}. Check Airflow logs.",
        subject="CDC Pipeline — FAILURE",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    dms_health_check >> [dms_not_running, schema_drift_check]
    schema_drift_check >> [drift_alert, glue_cdc]
    glue_cdc >> dq_checks >> merge >> dq_summary >> notify_success
    [dq_checks, merge] >> notify_failure
