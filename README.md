# Iceberg Table Metrics Collector

A Python utility for collecting and analyzing comprehensive metrics from Apache Iceberg tables. This tool extracts detailed statistics about snapshots, files, and partitions to help monitor table health, optimize performance, and identify potential issues.

## Overview

This utility collects three main categories of metrics from Iceberg tables:

1. **Snapshot Metrics**: Information about table changes and current state
2. **Files Metrics**: Statistics about data files within the table
3. **Partition Metrics**: Analysis of partition distribution and characteristics

## Metrics Collected

### Snapshot Metrics

| Metric | Description | Unit |
|--------|-------------|------|
| added_data_files | Number of data files added in the latest snapshot | Count |
| added_records | Number of records added in the latest snapshot | Count |
| changed_partition_count | Number of partitions modified in the latest snapshot | Count |
| total_records | Total number of records in the table | Count |
| total_data_files | Total number of data files in the table | Count |
| total_delete_files | Number of delete files in the table | Count |
| added_files_size | Size of newly added files | Bytes |
| total_files_size | Total size of all files in the table | Bytes |
| added_position_deletes | Number of position delete records added | Count |

### Files Metrics

| Metric | Description | Unit |
|--------|-------------|------|
| avg_record_count | Average number of records per file | Count |
| max_record_count | Maximum number of records in any file | Count |
| min_record_count | Minimum number of records in any file | Count |
| avg_file_size | Average file size | Bytes |
| max_file_size | Size of the largest file | Bytes |
| min_file_size | Size of the smallest file | Bytes |

### Partition Metrics

| Metric | Description | Unit |
|--------|-------------|------|
| total_partitions | Total number of partitions in the table | Count |
| avg_record_count | Average number of records per partition | Count |
| max_record_count | Maximum number of records in any partition | Count |
| min_record_count | Minimum number of records in any partition | Count |
| deviation_record_count | Standard deviation of record counts across partitions | Count |
| skew_record_count | Skewness of record distribution across partitions | Count |
| avg_file_count | Average number of files per partition | Count |
| max_file_count | Maximum number of files in any partition | Count |
| min_file_count | Minimum number of files in any partition | Count |
| total_size_bytes | Total size of all partitions | Bytes |

## Setup and Configuration

### Required Configuration

Before using this utility, you need to:

1. **Set your Spark configuration**: Configure the Spark session with proper Iceberg support
2. **Set your warehouse path**: Specify the S3 or local path where your Iceberg tables are stored

```python
# Example minimum configuration
import os
from iceberg_metrics import collect_all_metrics, collect_metrics_for_table

# Set warehouse path - REQUIRED
WAREHOUSE = "arn:aws:s3tables:us-east-1:YOUR_ACCOUNT_ID:bucket/YOUR_BUCKET"

# Optional: Set Java home if needed for local development
os.environ["JAVA_HOME"] = "/path/to/your/java"
```

### Spark Configuration

The utility automatically sets up a Spark session with the following configuration:

```python
conf = {
    "spark.app.name": "iceberg_metrics",
    "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.ManagedIcebergCatalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ManagedIcebergCatalog.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "spark.sql.catalog.ManagedIcebergCatalog.warehouse": WAREHOUSE,
    "spark.sql.catalog.ManagedIcebergCatalog.client.region": "us-east-1",
}
```

You can customize this configuration as needed for your environment.

## Usage

### Available Methods

The utility provides two main methods:

1. **`collect_all_metrics()`**: Collects metrics for all tables in a specified catalog and database
2. **`collect_metrics_for_table()`**: Collects metrics for a single specified table

### Running the Utility

**Option 1: As a standalone script**

```python
# Set your warehouse path first
if __name__ == "__main__":
    global WAREHOUSE
    WAREHOUSE = "arn:aws:s3tables:us-east-1:YOUR_ACCOUNT_ID:bucket/YOUR_BUCKET"
    
    # Collect metrics for all tables
    results = collect_all_metrics(catalog="ManagedIcebergCatalog", database="s3tablescatalog")
    
    # OR collect metrics for a specific table
    table_metrics = collect_metrics_for_table("customers", 
                                             catalog="ManagedIcebergCatalog", 
                                             database="s3tablescatalog")
```

**Option 2: Import in your code**

```python
from iceberg_metrics import collect_all_metrics, collect_metrics_for_table

# Set your warehouse path
WAREHOUSE = "arn:aws:s3tables:us-east-1:YOUR_ACCOUNT_ID:bucket/YOUR_BUCKET"

# Method 1: Collect metrics for all tables in a catalog and database
results = collect_all_metrics(catalog="MyCatalog", database="MyDatabase")

# Method 2: Collect metrics for a specific table
table_metrics = collect_metrics_for_table("my_table", 
                                         catalog="MyCatalog", 
                                         database="MyDatabase")
```

### Output Examples

Sample output for a table:

```
=== METRICS FOR TABLE s3tablescatalog.customers ===
{
  "snapshot": {
    "metrics": {
      "added_data_files": {
        "value": 5,
        "unit": "Count"
      },
      "added_records": {
        "value": 5,
        "unit": "Count"
      },
      "changed_partition_count": {
        "value": 1,
        "unit": "Count"
      },
      "total_records": {
        "value": 5,
        "unit": "Count"
      },
      "total_data_files": {
        "value": 5,
        "unit": "Count"
      },
      "total_delete_files": {
        "value": 0,
        "unit": "Count"
      },
      "added_files_size": {
        "value": 5054,
        "unit": "Bytes"
      },
      "total_files_size": {
        "value": 5054,
        "unit": "Bytes"
      },
      "added_position_deletes": {
        "value": 0,
        "unit": "Count"
      }
    },
    "timestamp": 1683216000000
  },
  "files": {
    "metrics": {
      "avg_record_count": {
        "value": 1,
        "unit": "Count"
      },
      "max_record_count": {
        "value": 1,
        "unit": "Count"
      },
      "min_record_count": {
        "value": 1,
        "unit": "Count"
      },
      "avg_file_size": {
        "value": 1010,
        "unit": "Bytes"
      },
      "max_file_size": {
        "value": 1044,
        "unit": "Bytes"
      },
      "min_file_size": {
        "value": 989,
        "unit": "Bytes"
      }
    },
    "timestamp": 1683216000000
  },
  "partitions": {
    "metrics": {
      "total_partitions": {
        "value": 1,
        "unit": "Count"
      },
      "avg_record_count": {
        "value": 5,
        "unit": "Count"
      },
      "max_record_count": {
        "value": 5,
        "unit": "Count"
      },
      "min_record_count": {
        "value": 5,
        "unit": "Count"
      },
      "deviation_record_count": {
        "value": 0.0,
        "unit": "Count"
      },
      "skew_record_count": {
        "value": 0.0,
        "unit": "Count"
      },
      "avg_file_count": {
        "value": 5,
        "unit": "Count"
      },
      "max_file_count": {
        "value": 5,
        "unit": "Count"
      },
      "min_file_count": {
        "value": 5,
        "unit": "Count"
      },
      "total_size_bytes": {
        "value": 5054,
        "unit": "Bytes"
      }
    },
    "timestamp": 1683216000000
  }
}
==================================================
```

## Use Cases

### Monitoring

- Track table growth over time
- Monitor data file counts and sizes
- Detect changes in partition distribution

### Optimization

- Identify tables with suboptimal file sizes
- Find partitions with high skew
- Track effectiveness of compaction operations

### Troubleshooting

- Detect tables with abnormal delete file counts
- Identify partitions with unusually high or low record counts
- Monitor metrics after schema evolution or other table operations

## Requirements

- PySpark 3.4+
- Apache Iceberg 1.6+
- pandas
- numpy
- boto3 (for AWS access)

## Extending

The utility can be extended to:

1. Store metrics in a time-series database
2. Visualize metrics with dashboarding tools
3. Set up alerts based on metric thresholds
4. Add custom metrics specific to your use case

## License

[MIT License](LICENSE)
