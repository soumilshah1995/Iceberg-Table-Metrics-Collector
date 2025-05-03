import json
import boto3
from datetime import datetime, timezone
import os
import time
import uuid
import pandas as pd
import numpy as np
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

GLOBAL_METRICS = {}


def create_spark_session():
    """
    Create a Spark session with proper configuration for Iceberg tables
    """

    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def record_metric(table_name, metric_category, metric_name, value, unit, timestamp=None):
    """
    Record a metric in the global metrics dictionary
    """
    # Initialize table entry if it doesn't exist
    if table_name not in GLOBAL_METRICS:
        GLOBAL_METRICS[table_name] = {}

    # Initialize category if it doesn't exist
    if metric_category not in GLOBAL_METRICS[table_name]:
        GLOBAL_METRICS[table_name][metric_category] = {
            "metrics": {},
            "timestamp": timestamp
        }

    # Record the metric value
    GLOBAL_METRICS[table_name][metric_category]["metrics"][metric_name] = {
        "value": value,
        "unit": unit
    }

    # Log the metric for debugging
    ts_str = f" at {datetime.fromtimestamp(timestamp / 1000.0).isoformat()}" if timestamp else ""
    logger.info(f"METRIC: {table_name} - {metric_category}.{metric_name} = {value} {unit}{ts_str}")


def get_snapshot_metrics(spark, catalog, database, table_name):
    """
    Get snapshot metrics for a table
    """
    logger.info(f"Getting snapshot metrics for {catalog}.{database}.{table_name}")

    # Query snapshots view
    snapshots_df = spark.sql(
        f"SELECT * FROM {catalog}.{database}.{table_name}.snapshots ORDER BY committed_at DESC LIMIT 1")

    # Add debugging info
    logger.info("Snapshot data from Spark:")

    if snapshots_df.count() == 0:
        logger.warning(f"No snapshots found for {catalog}.{database}.{table_name}")
        return None

    # Fix for datetime issue - convert committed_at to string in Spark before pandas conversion
    snapshots_df = snapshots_df.withColumn("committed_at", F.date_format("committed_at", "yyyy-MM-dd HH:mm:ss.SSS"))

    # Convert to pandas for easier handling
    try:
        snapshots_pd = snapshots_df.toPandas()
        logger.info(f"Successfully converted snapshot data to pandas DataFrame with {len(snapshots_pd)} rows")
        snapshot = snapshots_pd.iloc[0]
    except Exception as e:
        logger.error(f"Error converting snapshot data to pandas: {e}")
        # Alternative approach - extract the data directly from Spark
        snapshots_row = snapshots_df.collect()[0]
        snapshot = {field.name: snapshots_row[field.name] for field in snapshots_df.schema.fields}

    # Convert committed_at to timestamp in milliseconds - IMPROVED FIX
    logger.info(f"Processing committed_at value: {snapshot['committed_at']} of type {type(snapshot['committed_at'])}")

    # Safe way to access committed_at based on snapshot type
    if isinstance(snapshot, dict):
        committed_at = snapshot.get('committed_at')
    else:
        committed_at = getattr(snapshot, 'committed_at', None)

    # Handle different types of committed_at values properly
    if committed_at is None:
        # Fallback to current time if value is missing
        logger.warning("committed_at value is None, using current time")
        timestamp_ms = int(time.time() * 1000)
    elif isinstance(committed_at, str):
        try:
            # Try different string formats
            if 'Z' in committed_at or 'T' in committed_at or '+' in committed_at:
                # ISO format handling
                committed_at = committed_at.replace('Z', '+00:00')
                dt_obj = datetime.fromisoformat(committed_at.replace('T', ' ').split('+')[0].strip())
            else:
                # Standard format with potential fractional seconds
                if '.' in committed_at:
                    dt_obj = datetime.strptime(committed_at, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    dt_obj = datetime.strptime(committed_at, "%Y-%m-%d %H:%M:%S")

            timestamp_ms = int(dt_obj.timestamp() * 1000)
            logger.info(f"Successfully parsed committed_at string to timestamp: {timestamp_ms}")
        except Exception as e:
            # Fallback with detailed error message
            logger.error(f"Error parsing committed_at string '{committed_at}': {e}")
            timestamp_ms = int(time.time() * 1000)
    elif hasattr(committed_at, 'timestamp'):
        # If it's already a datetime object
        timestamp_ms = int(committed_at.timestamp() * 1000)
    else:
        # Last resort fallback
        logger.warning(
            f"Could not parse committed_at value: {committed_at} of type {type(committed_at)}, using current time")
        timestamp_ms = int(time.time() * 1000)

    # Extract metrics from summary column with improved error handling
    try:
        # Safely access the summary based on snapshot type
        if isinstance(snapshot, dict):
            summary_raw = snapshot.get('summary')
        else:
            summary_raw = getattr(snapshot, 'summary', None)

        logger.info(f"Raw summary data: {type(summary_raw)}")

        if summary_raw is None:
            logger.warning("Summary is None, using empty dict")
            summary = {}
        elif isinstance(summary_raw, str):
            try:
                summary = json.loads(summary_raw)
                logger.info("Successfully parsed summary from JSON string")
            except json.JSONDecodeError:
                logger.error(f"Failed to parse summary JSON: {summary_raw}")
                # Try to extract key-value pairs with regex if it has a pattern like {key->value, key2->value2}
                if summary_raw.startswith('{') and summary_raw.endswith('}'):
                    import re
                    pairs = re.findall(r'(\w+(?:-\w+)*)\s*->\s*([^,}]+)', summary_raw)
                    summary = {k.strip(): v.strip() for k, v in pairs}
                    logger.info(f"Extracted {len(summary)} key-value pairs from summary string")
                else:
                    summary = {}
        elif isinstance(summary_raw, dict):
            summary = summary_raw
        else:
            logger.warning(f"Unexpected summary type: {type(summary_raw)}, using empty dict")
            summary = {}
    except Exception as e:
        logger.error(f"Error processing summary: {e}")
        summary = {}

    # Create dict of metrics to return
    metrics = {
        "snapshot_id": snapshot['snapshot_id'],
        "operation": snapshot['operation'],
        "timestamp_ms": timestamp_ms,
        "summary": summary
    }

    metrics_to_monitor = [
        "added-data-files", "added-records", "changed-partition-count",
        "total-records", "total-data-files", "total-delete-files",
        "added-files-size", "total-files-size", "added-position-deletes"
    ]

    # Report each metric
    table_key = f"{database}.{table_name}"
    for metric in metrics_to_monitor:
        normalized_metric_name = metric.replace("-", "_")
        value = int(summary.get(metric, 0))
        unit = 'Bytes' if "size" in normalized_metric_name else "Count"

        # Record metrics in global dictionary
        record_metric(
            table_name=table_key,
            metric_category="snapshot",
            metric_name=normalized_metric_name,
            value=value,
            unit=unit,
            timestamp=timestamp_ms
        )

    return metrics


def get_files_metrics(spark, catalog, database, table_name, timestamp_ms):
    """
    Get file metrics for a table
    """
    logger.info(f"Getting files metrics for {catalog}.{database}.{table_name}")

    # Query files view
    files_query = f"""
        SELECT 
            CAST(AVG(record_count) as INT) as avg_record_count, 
            MAX(record_count) as max_record_count, 
            MIN(record_count) as min_record_count, 
            CAST(AVG(file_size_in_bytes) as INT) as avg_file_size, 
            MAX(file_size_in_bytes) as max_file_size, 
            MIN(file_size_in_bytes) as min_file_size 
        FROM {catalog}.{database}.{table_name}.files
    """

    try:
        files_df = spark.sql(files_query)

        if files_df.count() == 0:
            logger.warning(f"No files found for {catalog}.{database}.{table_name}")
            return None

        # Convert to pandas for easier handling
        file_metrics_pd = files_df.toPandas().iloc[0]

        # Create metrics dict - make sure to handle null values
        file_metrics = {
            "avg_record_count": int(file_metrics_pd["avg_record_count"]) if pd.notna(
                file_metrics_pd["avg_record_count"]) else 0,
            "max_record_count": int(file_metrics_pd["max_record_count"]) if pd.notna(
                file_metrics_pd["max_record_count"]) else 0,
            "min_record_count": int(file_metrics_pd["min_record_count"]) if pd.notna(
                file_metrics_pd["min_record_count"]) else 0,
            "avg_file_size": int(file_metrics_pd["avg_file_size"]) if pd.notna(file_metrics_pd["avg_file_size"]) else 0,
            "max_file_size": int(file_metrics_pd["max_file_size"]) if pd.notna(file_metrics_pd["max_file_size"]) else 0,
            "min_file_size": int(file_metrics_pd["min_file_size"]) if pd.notna(file_metrics_pd["min_file_size"]) else 0,
        }

        # Report each metric
        table_key = f"{database}.{table_name}"
        for metric_name, metric_value in file_metrics.items():
            unit = 'Bytes' if "size" in metric_name else "Count"

            # Record metrics in global dictionary
            record_metric(
                table_name=table_key,
                metric_category="files",
                metric_name=metric_name,
                value=metric_value,
                unit=unit,
                timestamp=timestamp_ms
            )

        return file_metrics

    except Exception as e:
        logger.error(f"Error getting file metrics: {e}")
        return None


def get_partition_metrics(spark, catalog, database, table_name, timestamp_ms):
    """
    Get partition metrics for a table
    """
    logger.info(f"Getting partition metrics for {catalog}.{database}.{table_name}")

    try:
        # Modified query to use available columns
        partitions_query = f"""
            SELECT 
                record_count,
                file_count,
                CAST(total_data_file_size_in_bytes AS BIGINT) as file_size
            FROM {catalog}.{database}.{table_name}.partitions
        """
        partitions_df = spark.sql(partitions_query)

        if partitions_df.count() == 0:
            logger.info(f"No partitions found for {catalog}.{database}.{table_name}")
            return None

        # Convert to pandas for easier calculations
        partitions_pd = partitions_df.toPandas()

        # Make sure columns are numeric
        partitions_pd = partitions_pd.apply(pd.to_numeric, errors='coerce').fillna(0)

        # Calculate aggregate statistics
        partition_metrics = {
            "total_partitions": len(partitions_pd),
            "avg_record_count": int(partitions_pd["record_count"].mean()),
            "max_record_count": int(partitions_pd["record_count"].max()),
            "min_record_count": int(partitions_pd["record_count"].min()),
            "deviation_record_count": round(float(partitions_pd["record_count"].std() or 0), 2),
            "skew_record_count": round(float(partitions_pd["record_count"].skew() if len(partitions_pd) > 2 else 0), 2),
            "avg_file_count": int(partitions_pd["file_count"].mean()),
            "max_file_count": int(partitions_pd["file_count"].max()),
            "min_file_count": int(partitions_pd["file_count"].min()),
            "total_size_bytes": int(partitions_pd["file_size"].sum())
        }

        # Report metrics
        table_key = f"{database}.{table_name}"

        for metric_name, metric_value in partition_metrics.items():
            unit = "Bytes" if "size" in metric_name else "Count"

            # Record metrics in global dictionary
            record_metric(
                table_name=table_key,
                metric_category="partitions",
                metric_name=metric_name,
                value=metric_value,
                unit=unit,
                timestamp=timestamp_ms
            )

        return partition_metrics

    except Exception as e:
        logger.error(f"Error getting partition metrics: {e}")
        return None


def collect_table_metrics(spark, catalog, database, table_name):
    """
    Collect all metrics for a given table
    """
    logger.info(f"Collecting metrics for {catalog}.{database}.{table_name}")

    # Get snapshot metrics first to get timestamp
    try:
        snapshot_metrics = get_snapshot_metrics(spark, catalog, database, table_name)

        if not snapshot_metrics:
            logger.warning(f"No snapshots found for {catalog}.{database}.{table_name}. Skipping additional metrics.")
            return None

        timestamp_ms = snapshot_metrics["timestamp_ms"]

        # Get file metrics
        file_metrics = get_files_metrics(spark, catalog, database, table_name, timestamp_ms)

        # Get partition metrics
        partition_metrics = get_partition_metrics(spark, catalog, database, table_name, timestamp_ms)

        # Print collected metrics for this table
        table_key = f"{database}.{table_name}"
        if table_key in GLOBAL_METRICS:
            print(f"\n=== METRICS FOR TABLE {table_key} ===")
            print(json.dumps(GLOBAL_METRICS[table_key], indent=2))
            print("=" * 50)

        return {
            "snapshot": snapshot_metrics,
            "files": file_metrics,
            "partitions": partition_metrics
        }
    except Exception as e:
        logger.error(f"Error collecting metrics for table {table_name}: {e}")
        return None


def collect_all_metrics(catalog="ManagedIcebergCatalog", database="s3tablescatalog"):
    """
    Collect metrics for all tables in a given catalog and database
    """
    logger.info(f"Starting metrics collection for {catalog}.{database}")

    # Create Spark session
    spark = create_spark_session()

    # Show available schemas for reference
    logger.info("Available schemas:")
    spark.sql(f"SHOW SCHEMAS IN {catalog}").show()

    # Show available tables
    logger.info(f"Available tables in {catalog}.{database}:")
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{database}")
    tables_df.show()

    # Process each table
    tables = tables_df.select("tableName").collect()
    results = {}

    for row in tables:
        table_name = row["tableName"]
        logger.info(f"Processing table: {table_name}")

        # Check if table exists and is an Iceberg table
        try:
            # Try to access an Iceberg metadata view to validate
            test_query = f"SELECT * FROM {catalog}.{database}.{table_name}.snapshots LIMIT 1"

            # Collect metrics
            table_metrics = collect_table_metrics(spark, catalog, database, table_name)
            results[table_name] = table_metrics

        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")
            continue

    # Stop Spark session
    spark.stop()

    # Print all collected metrics
    print("\n===== ALL COLLECTED METRICS =====")
    print(json.dumps(GLOBAL_METRICS, indent=2))
    print("=" * 50)

    return results


def collect_metrics_for_table(table_name, catalog="ManagedIcebergCatalog", database="s3tablescatalog"):
    """
    Collect metrics for a specific table
    """
    logger.info(f"Starting metrics collection for {catalog}.{database}.{table_name}")
    spark = create_spark_session()

    try:
        # Validate table exists and is an Iceberg table
        test_query = f"SELECT * FROM {catalog}.{database}.{table_name}.snapshots LIMIT 1"
        spark.sql(test_query)

        # Use current timestamp for metrics
        current_timestamp_ms = int(time.time() * 1000)

        # Collect metrics
        table_metrics = {
            "snapshot": get_snapshot_metrics(spark, catalog, database, table_name),
            "files": get_files_metrics(spark, catalog, database, table_name, current_timestamp_ms),
            "partitions": get_partition_metrics(spark, catalog, database, table_name, current_timestamp_ms)
        }

        # Print collected metrics for this table
        table_key = f"{database}.{table_name}"
        if table_key in GLOBAL_METRICS:
            print(f"\n=== METRICS FOR TABLE {table_key} ===")
            print(json.dumps(GLOBAL_METRICS[table_key], indent=2))
            print("=" * 50)

        spark.stop()
        return table_metrics

    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        spark.stop()
        return None


# if __name__ == "__main__":
#     global WAREHOUSE, conf

#     WAREHOUSE = "arn:aws:s3tables:us-east-1:XX:bucket/XXXX-dev"
#     os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
#     conf = {
#         "spark.app.name": "iceberg_metrics",
#         "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
#         "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#         "spark.sql.catalog.ManagedIcebergCatalog": "org.apache.iceberg.spark.SparkCatalog",
#         "spark.sql.catalog.ManagedIcebergCatalog.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
#         "spark.sql.catalog.ManagedIcebergCatalog.warehouse": WAREHOUSE,
#         "spark.sql.catalog.ManagedIcebergCatalog.client.region": "us-east-1",
#     }

#     # # # Collect metrics for all tables
#     results = collect_all_metrics(catalog="ManagedIcebergCatalog", database="s3tablescatalog")

#     # # Collect metrics for a specific table
#     table_metrics = collect_metrics_for_table("customers", catalog="ManagedIcebergCatalog", database="s3tablescatalog")
