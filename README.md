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

## Usage

### Running the Utility

```python
  global WAREHOUSE

  WAREHOUSE =""

  # # # Collect metrics for all tables
  # results = collect_all_metrics(catalog="ManagedIcebergCatalog", database="s3tablescatalog")
  # 
  # # Collect metrics for a specific table
  # table_metrics = collect_metrics_for_table("customers", catalog="ManagedIcebergCatalog", database="s3tablescatalog")

```

### Output Examples

Sample output for a table:

```
Table: s3tablescatalog.customers
  Snapshot metrics:
    added_data_files: 5 Count
    added_records: 5 Count
    changed_partition_count: 1 Count
    total_records: 5 Count
    total_data_files: 5 Count
    total_delete_files: 0 Count
    added_files_size: 5054 Bytes
    total_files_size: 5054 Bytes
    added_position_deletes: 0 Count
  Files metrics:
    avg_record_count: 1 Count
    max_record_count: 1 Count
    min_record_count: 1 Count
    avg_file_size: 1010 Bytes
    max_file_size: 1044 Bytes
    min_file_size: 989 Bytes
  Partition metrics:
    total_partitions: 1 Count
    avg_record_count: 5 Count
    max_record_count: 5 Count
    min_record_count: 5 Count
    deviation_record_count: nan Count
    skew_record_count: 0.0 Count
    avg_file_count: 5 Count
    max_file_count: 5 Count
    min_file_count: 5 Count
    total_size_bytes: 5054 Bytes
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
- Apache Iceberg 1.3+
- pandas
- numpy

## Configuration

The utility requires proper Spark configuration with Iceberg support:

```python
# Example Spark configuration
conf = {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.MyCatalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.MyCatalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.MyCatalog.warehouse": "s3://my-bucket/warehouse"
}
```

## Extending

The utility can be extended to:

1. Store metrics in a time-series database
2. Visualize metrics with dashboarding tools
3. Set up alerts based on metric thresholds
4. Add custom metrics specific to your use case

## License

[MIT License](LICENSE)
