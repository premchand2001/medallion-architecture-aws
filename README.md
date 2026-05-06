# medallion-architecture-aws

Production-grade Medallion Architecture (Bronze / Silver / Gold) on Amazon S3 using Delta Lake, AWS Glue, EMR, Apache Airflow (MWAA), and Snowflake — built from real healthcare data engineering work at Optum (UnitedHealth Group).

## Architecture Overview

```
Raw Sources (CSV/JSON/flat files)
        │
        ▼
┌──────────────┐
│   BRONZE     │  Raw ingestion, no transforms, full audit trail
│  S3 + Delta  │  + _ingestion_timestamp, _record_hash, _source_file
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   SILVER     │  Cleaned, validated, deduplicated
│  S3 + Delta  │  Schema enforcement, null handling, DQ checks
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    GOLD      │  Star-schema aggregations for BI consumers
│  Snowflake   │  Loaded into Snowflake for reporting & analytics
└──────────────┘
```

## Stack

| Layer | Technology |
|-------|-----------|
| Storage | Amazon S3 |
| Table Format | Delta Lake |
| Processing | AWS Glue, AWS EMR (PySpark) |
| Orchestration | Apache Airflow on Amazon MWAA |
| Warehouse | Snowflake |
| CI/CD | GitHub Actions |

## Files

```
medallion-architecture-aws/
├── medallion_pipeline.py       # Core Bronze/Silver/Gold PySpark logic
├── dags/
│   └── medallion_dag.py        # Airflow DAG for end-to-end orchestration
├── glue_jobs/
│   └── bronze_ingestion.py     # AWS Glue job for raw ingestion
└── README.md
```

## Key Features

- **Bronze Layer** — Raw ingestion with full audit metadata (`_record_hash`, `_ingestion_timestamp`, `_source_file`). Delta Lake merge prevents duplicate loads.
- **Silver Layer** — PySpark transformations with automated data quality checks: null %, duplicate detection, schema drift alerts. Deduplication via SHA-256 record hash.
- **Gold Layer** — Star-schema fact tables aggregated from Silver and loaded into Snowflake for BI team consumption.
- **Airflow DAG** — S3 sensor → Glue bronze job → EMR silver step → EMR gold step → Snowflake validation. Full retry logic, SLA monitoring, and email alerting.

## Data Quality Checks (Silver Layer)

- Null percentage per column (warns if > 10%)
- Duplicate record detection via `_record_hash`
- String standardization (trim, uppercase)
- Schema validation on write

## Setup

1. Deploy Airflow on Amazon MWAA
2. Set Airflow variables: `emr_cluster_id`
3. Configure Airflow connections: `aws_default`, `snowflake_default`
4. Upload `medallion_pipeline.py` to your scripts S3 bucket
5. Place DAG in your MWAA DAGs S3 bucket

## Based On

Real work from the **Optum — Enterprise Data Platform** project (2022–2023), processing healthcare datasets across multiple business units using Delta Lake on Amazon S3.
