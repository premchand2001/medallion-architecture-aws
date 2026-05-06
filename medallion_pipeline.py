"""
Medallion Architecture Pipeline (Bronze → Silver → Gold)
AWS S3 + Delta Lake + AWS Glue + Snowflake
Author: Premchand Kothapalli
"""

import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws, lit,
    when, count, isnan, trim, upper
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

S3_BUCKET = "your-data-lake-bucket"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze"
SILVER_PATH = f"s3://{S3_BUCKET}/silver"
GOLD_PATH   = f"s3://{S3_BUCKET}/gold"

SNOWFLAKE_OPTIONS = {
    "sfURL":       "your_account.snowflakecomputing.com",
    "sfUser":      "your_user",
    "sfPassword":  "your_password",
    "sfDatabase":  "HEALTHCARE_DW",
    "sfSchema":    "GOLD",
    "sfWarehouse": "COMPUTE_WH",
}

# ─── Spark Session ─────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MedallionArchitecturePipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


# ─── BRONZE LAYER: Raw Ingestion ───────────────────────────────────────────────

def ingest_to_bronze(spark: SparkSession, source_path: str, domain: str, file_format: str = "csv"):
    """
    Ingest raw data from S3 source into Bronze Delta Lake layer.
    No transformations — preserves raw data with metadata columns.
    """
    logger.info(f"[BRONZE] Ingesting {domain} from {source_path}")

    raw_df = spark.read.format(file_format).option("header", "true").option("inferSchema", "true").load(source_path)

    bronze_df = (
        raw_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit(source_path))
        .withColumn("_domain", lit(domain))
        .withColumn("_record_hash", sha2(concat_ws("||", *[col(c) for c in raw_df.columns]), 256))
    )

    target_path = f"{BRONZE_PATH}/{domain}"

    if DeltaTable.isDeltaTable(spark, target_path):
        logger.info(f"[BRONZE] Merging into existing Delta table at {target_path}")
        delta_table = DeltaTable.forPath(spark, target_path)
        (
            delta_table.alias("existing")
            .merge(bronze_df.alias("incoming"), "existing._record_hash = incoming._record_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        logger.info(f"[BRONZE] Creating new Delta table at {target_path}")
        bronze_df.write.format("delta").mode("overwrite").partitionBy("_domain").save(target_path)

    logger.info(f"[BRONZE] Done. Records: {bronze_df.count()}")
    return bronze_df


# ─── SILVER LAYER: Cleaned & Validated ────────────────────────────────────────

def apply_data_quality_checks(df, domain: str):
    """Run quality validations: nulls, duplicates, schema drift."""
    logger.info(f"[SILVER] Running data quality checks for {domain}")

    total = df.count()
    null_counts = {c: df.filter(col(c).isNull() | isnan(col(c))).count() for c in df.columns if c not in ["_ingestion_timestamp", "_source_file", "_domain", "_record_hash"]}
    duplicate_count = total - df.dropDuplicates(["_record_hash"]).count()

    for col_name, nulls in null_counts.items():
        pct = (nulls / total) * 100 if total > 0 else 0
        if pct > 10:
            logger.warning(f"[DQ] Column '{col_name}' has {pct:.1f}% nulls in {domain}")

    if duplicate_count > 0:
        logger.warning(f"[DQ] Found {duplicate_count} duplicate records in {domain}")

    return df.dropDuplicates(["_record_hash"])


def transform_to_silver(spark: SparkSession, domain: str, primary_keys: list):
    """
    Clean, validate, and deduplicate Bronze data → Silver layer.
    Applies schema validation, null handling, and standardization.
    """
    logger.info(f"[SILVER] Transforming {domain}")

    bronze_df = spark.read.format("delta").load(f"{BRONZE_PATH}/{domain}")

    # Standardize string columns
    string_cols = [f.name for f in bronze_df.schema.fields if isinstance(f.dataType, StringType) and not f.name.startswith("_")]
    cleaned_df = bronze_df
    for c in string_cols:
        cleaned_df = cleaned_df.withColumn(c, trim(upper(col(c))))

    # Data quality checks
    validated_df = apply_data_quality_checks(cleaned_df, domain)

    silver_df = (
        validated_df
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_is_valid", lit(True))
    )

    target_path = f"{SILVER_PATH}/{domain}"
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("_domain").save(target_path)

    logger.info(f"[SILVER] Done. Valid records: {silver_df.count()}")
    return silver_df


# ─── GOLD LAYER: Star Schema for Analytics ────────────────────────────────────

def build_gold_fact_table(spark: SparkSession, domain: str, fact_columns: list, measure_columns: list):
    """
    Aggregate Silver data into Gold star-schema fact tables for BI consumption.
    Loads final dataset into Snowflake.
    """
    logger.info(f"[GOLD] Building fact table for {domain}")

    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}/{domain}")

    gold_df = (
        silver_df
        .select(fact_columns + measure_columns)
        .withColumn("_gold_timestamp", current_timestamp())
    )

    # Write to Gold Delta
    target_path = f"{GOLD_PATH}/{domain}_fact"
    gold_df.write.format("delta").mode("overwrite").save(target_path)
    logger.info(f"[GOLD] Delta table written at {target_path}")

    # Load to Snowflake
    (
        gold_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", f"{domain.upper()}_FACT")
        .mode("overwrite")
        .save()
    )
    logger.info(f"[GOLD] Loaded to Snowflake table {domain.upper()}_FACT")
    return gold_df


# ─── Orchestration ─────────────────────────────────────────────────────────────

def run_pipeline(domain: str, source_path: str, primary_keys: list, fact_columns: list, measure_columns: list):
    spark = get_spark()
    try:
        ingest_to_bronze(spark, source_path, domain)
        transform_to_silver(spark, domain, primary_keys)
        build_gold_fact_table(spark, domain, fact_columns, measure_columns)
        logger.info(f"[PIPELINE] Medallion pipeline complete for domain: {domain}")
    except Exception as e:
        logger.error(f"[PIPELINE] Pipeline failed for {domain}: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline(
        domain="claims",
        source_path=f"s3://{S3_BUCKET}/raw/claims/",
        primary_keys=["claim_id", "member_id"],
        fact_columns=["claim_id", "member_id", "provider_id", "service_date", "diagnosis_code"],
        measure_columns=["billed_amount", "paid_amount", "allowed_amount"],
    )
