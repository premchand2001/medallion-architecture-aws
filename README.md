# medallion-architecture-aws

Production-grade Medallion Architecture (Bronze / Silver / Gold) on Amazon S3 using Delta Lake, AWS Glue, EMR PySpark, Apache Airflow on MWAA, and Snowflake. Built from real healthcare data engineering work at **Optum (UnitedHealth Group)**, processing **10M+ daily records** across multiple clinical business units.

---

## Why This Exists

Legacy Datastage and SSIS pipelines at Optum couldn't scale to the volume, schema variance, or audit requirements of modern healthcare data. This framework replaced them — moving to a fully cloud-native, ACID-compliant Delta Lake architecture where every record is traceable from raw source to gold-layer consumption.

---

## Architecture

```
Raw Sources
(CSV / JSON / Snowflake / SQL Server / flat files)
              │
              ▼
┌─────────────────────────────────────┐
│  BRONZE  —  S3 + Delta Lake         │
│                                     │
│  Raw ingestion, zero transformation │
│  Full audit metadata on every row:  │
│    _ingestion_timestamp             │
│    _record_hash  (SHA-256)          │
│    _source_file                     │
│                                     │
│  Delta MERGE prevents duplicate     │
│  loads on re-runs                   │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│  SILVER  —  S3 + Delta Lake         │
│                                     │
│  PySpark transformations on EMR     │
│  Schema enforcement on write        │
│  Automated DQ checks:               │
│    - Null % per column              │
│    - Duplicate detection            │
│    - String standardization         │
│    - Schema drift alerts            │
│  Deduplication via _record_hash     │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│  GOLD  —  Snowflake                 │
│                                     │
│  Star-schema aggregations           │
│  Loaded via Snowflake COPY INTO     │
│  Optimized for BI consumption       │
└─────────────────────────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Storage | Amazon S3 |
| Table Format | Delta Lake |
| Ingestion | AWS Glue (dynamic frames, schema evolution) |
| Processing | AWS EMR — PySpark (broadcast joins, partition pruning, caching) |
| Orchestration | Apache Airflow on Amazon MWAA |
| Warehouse | Snowflake |
| CI/CD | GitHub Actions |

---

## Repository Structure

```
medallion-architecture-aws/
├── medallion_pipeline.py        # Bronze / Silver / Gold PySpark logic
├── dags/
│   └── medallion_dag.py         # Airflow DAG — end-to-end orchestration
├── glue_jobs/
│   └── bronze_ingestion.py      # Glue job for raw source ingestion
└── README.md
```

---

## Airflow DAG — `medallion_dag.py`

```
check_source_data
└── trigger_glue_bronze_ingestion
    └── validate_bronze_s3          ← S3KeySensor
        └── create_emr_cluster
            └── submit_emr_steps
                ├── wait_bronze_to_silver
                └── wait_silver_to_gold
                    └── validate_row_counts
                        └── terminate_emr_cluster
                            └── load_snowflake
                                └── validate_snowflake_load
                                    └── notify_success / notify_failure
```

- **Schedule:** Daily at 02:00 UTC
- **SLA:** Must complete within 4 hours
- **Retries:** 2 retries with 15-minute delay
- **Alerting:** SNS on success and failure

---

## Bronze Layer — `glue_jobs/bronze_ingestion.py`

AWS Glue job handles raw ingestion with dynamic frames and schema evolution. Absorbs upstream changes from Snowflake, SQL Server, and flat file sources without breaking downstream Silver jobs. Every row gets:

- `_ingestion_timestamp` — wall-clock time of load
- `_record_hash` — SHA-256 of all business columns (used for deduplication in Silver)
- `_source_file` — origin file/table for full lineage

Delta MERGE ensures re-running the job on the same source data never creates duplicates.

---

## Silver Layer — `medallion_pipeline.py`

PySpark transformations on AWS EMR. Performance tuning for 10M+ daily record volumes:

- **Broadcast joins** for small dimension tables
- **Partition pruning** on date and business unit columns
- **Selective caching** for reused DataFrames across transformation steps

**Data Quality Checks (run before Silver write):**

| Check | Behaviour |
|---|---|
| Null % per column | Warns if > 10%, fails if > 30% |
| Duplicate records | Detected via `_record_hash`, logged and dropped |
| String standardization | Trim whitespace, uppercase normalization |
| Schema validation | Enforced on Delta write — mismatches raise exceptions |

---

## Gold Layer

Star-schema fact and dimension tables aggregated from Silver. Loaded into Snowflake using `COPY INTO` with full row-count validation post-load. Designed for direct BI consumption via Tableau and SQL reporting.

---

## CI/CD — GitHub Actions

- **Lint and unit test** on every push
- **Integration test** against dev MWAA on PR merge
- **Deploy to production MWAA** on version tag

Separate workflows for Glue job deployment, EMR bootstrap script updates, and Snowflake DDL migrations.

---

## Setup

1. Deploy Airflow on Amazon MWAA
2. Set Airflow variables: `emr_cluster_id`, `s3_data_lake_bucket`, `snowflake_schema`
3. Configure connections: `aws_default`, `snowflake_default`
4. Upload `medallion_pipeline.py` to your EMR bootstrap S3 bucket
5. Upload `bronze_ingestion.py` to your Glue scripts S3 path
6. Place `medallion_dag.py` in your MWAA DAGs S3 bucket

