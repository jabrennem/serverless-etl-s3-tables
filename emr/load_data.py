import json
import sys
import urllib.parse
from pyspark.sql import SparkSession


def _write_stats_file(
    stats_s3_uri: str,
    input_path: str,
    catalog_id: str,
    namespace: str,
    table_name: str,
    row_count: int,
) -> None:
    """Write per-table run stats to S3 for the state machine's Read Run Stats step."""
    try:
        import boto3
        parsed = urllib.parse.urlparse(stats_s3_uri)
        boto3.client("s3").put_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
            Body=json.dumps({
                "table": table_name,
                "rowCount": row_count,
                "input": input_path.removeprefix("s3://"),
                "output": f"{catalog_id}/{namespace}.{table_name}",
            }).encode(),
        )
        print(f"Stats written to {stats_s3_uri} ({row_count} rows)")
    except Exception as e:
        print(f"Warning: failed to write stats file: {e}")


def main(input_path: str, catalog_id: str, namespace: str,
         table_name: str, stats_s3_uri: str = "") -> int:
    """Load a Parquet file from S3 into an S3 Table Bucket (managed Iceberg) via Glue REST catalog.

    Args:
        input_path: S3 path to the source Parquet file.
        catalog_id: Glue catalog ID for the S3 Table Bucket (e.g. 123456789012:s3tablescatalog/bucket-name).
        namespace: Namespace within the table bucket.
        table_name: Name of the target table.
        stats_s3_uri: S3 URI to write run stats JSON (optional).

    Returns:
        0 on success.
    """
    iceberg_table = f"s3tablesbucket.{namespace}.{table_name}"

    print(f"Loading {input_path} into {iceberg_table}")
    print(f"Glue catalog ID: {catalog_id}")

    spark = (
        SparkSession.builder
        .appName(f"S3TableBucket-Import-{table_name}")
        .getOrCreate()
    )

    df = spark.read.parquet(input_path)
    print(f"Read Parquet file: {input_path}")

    count = df.count()
    print(f"Found {count} records")
    df.printSchema()

    # Write data — overwrite the pre-existing table
    df.createOrReplaceTempView("source_data")
    spark.sql(f"INSERT OVERWRITE {iceberg_table} SELECT * FROM source_data")
    print(f"Data successfully written to '{iceberg_table}' ({count} rows)")

    if stats_s3_uri:
        _write_stats_file(stats_s3_uri, input_path, catalog_id, namespace, table_name, count)

    spark.stop()
    return 0


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) < 5:
        print("Usage: load_data.py <input_path> <catalog_id> <namespace> <table_name> [stats_s3_uri]")
        sys.exit(1)
    stats_s3_uri = sys.argv[5] if len(sys.argv) > 5 else ""
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], stats_s3_uri))
