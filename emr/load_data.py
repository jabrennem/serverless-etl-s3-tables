import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType


def _write_stats_file(spark: SparkSession, stats_s3_uri: str, results: list) -> None:
    """Write per-group run stats as JSON to S3 via Spark's Hadoop filesystem.

    Uses EMRFS (the same S3 client Spark uses for reads/writes) rather than
    boto3, which cannot reach S3 from the EMR Serverless driver container.
    """
    try:
        stats = json.dumps({"files": results, "fileCount": len(results)})
        path = spark._jvm.org.apache.hadoop.fs.Path(stats_s3_uri)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        out = fs.create(path, True)
        out.write(bytearray(stats.encode()))
        out.close()
        print(f"Stats written to {stats_s3_uri} ({len(results)} files)")
    except Exception as e:
        print(f"Warning: failed to write stats file: {e}")


def _parse_batch(batch_arg):
    """Coerce the batch argument into a list of {SourceFile, TableName} dicts.

    Step Functions may hand us the batch already decoded (a list), a JSON string,
    or — depending on how JSONata stringifies it — a JSON string that itself
    decodes to another JSON string (double-encoded). Decode until we have a list.
    """
    val = batch_arg
    # Decode JSON strings, tolerating one or more layers of encoding.
    for _ in range(3):
        if isinstance(val, str):
            val = json.loads(val)
        else:
            break
    # A single-file group may arrive as a bare object rather than a 1-element
    # array (e.g. JSONata's $map over a one-item sequence). Coerce to a list.
    if isinstance(val, dict):
        val = [val]
    if not isinstance(val, list):
        raise ValueError(f"Expected a list of file items, got {type(val).__name__}: {val!r}")
    return val


def main(batch_json, catalog_id: str, namespace: str, stats_s3_uri: str = "") -> int:
    """Load a batch of Parquet files into an S3 Table Bucket (managed Iceberg) in ONE job.

    A single EMR Serverless job — one Spark session — processes every file in the
    group, amortizing the per-job startup cost across the batch. Each file is
    loaded into its own Iceberg table: before appending, the table is checked for
    the file's S3 key (source_file), and the append is skipped if it is already
    present. Re-running a state machine after a partial failure re-sends the same
    file list, so already-committed files are skipped rather than double-appended.
    One job run may write to several different tables.

    Args:
        batch_json: JSON array (or JSON string of one) of
            {"SourceFile": <s3 uri>, "TableName": <table>} items.
        catalog_id: Glue catalog ID for the S3 Table Bucket (e.g. 123456789012:s3tablescatalog/bucket-name).
        namespace: Namespace within the table bucket.
        stats_s3_uri: S3 URI to write run stats JSON (optional).

    Returns:
        0 if every file committed; 1 if any file failed (after attempting all).
    """
    batch = _parse_batch(batch_json)
    print(f"Batch of {len(batch)} file(s) into catalog {catalog_id}")

    # One ingestion timestamp for the whole job run — every row loaded by this
    # job carries the same ingest_datetime, marking when the batch was ingested.
    ingest_dt = datetime.now(timezone.utc)
    print(f"ingest_datetime for this run: {ingest_dt.isoformat()}")

    spark = (
        SparkSession.builder
        .appName(f"S3TableBucket-Batch-{len(batch)}files")
        .getOrCreate()
    )

    results = []
    failed = False

    for item in batch:
        input_path = item["SourceFile"]
        table_name = item["TableName"]
        iceberg_table = f"s3tablesbucket.{namespace}.{table_name}"

        try:
            print(f"Loading {input_path} into {iceberg_table}")
            df = spark.read.parquet(input_path)
            count = df.count()
            print(f"  read {count} records")

            # Stamp every row with the batch-run ingestion time (audit column)
            # and the full source S3 URI (idempotency + lineage key).
            source_uri = input_path  # s3://bucket/feed/....parquet
            df = (
                df
                .withColumn("ingest_datetime", lit(ingest_dt).cast(TimestampType()))
                .withColumn("source_file", lit(source_uri))
            )

            # Idempotent load: check whether this file's rows are already in the
            # table (keyed on source_file), and append only if not. Re-running a
            # state machine after a partial failure re-sends the same file list,
            # so already-committed files are skipped and never double-appended.
            # A plain SELECT (LF SELECT works) + append (write path that works)
            # avoids the MERGE write-commit credential path that 403'd on this
            # S3 Tables + Lake Formation setup. Predicate on source_file lets
            # Iceberg prune by column stats rather than scan the whole table.
            escaped_uri = source_uri.replace("'", "''")
            already_loaded = spark.sql(
                f"SELECT 1 FROM {iceberg_table} "
                f"WHERE source_file = '{escaped_uri}' LIMIT 1"
            ).count() > 0

            if already_loaded:
                print(f"  skip {source_uri} — already loaded into '{iceberg_table}'")
                results.append({
                    "table": table_name,
                    "input": input_path.removeprefix("s3://"),
                    "rowCount": 0,
                    "status": "skipped",
                })
                continue

            df.writeTo(iceberg_table).append()
            print(f"  appended {count} rows to '{iceberg_table}' (key: {source_uri})")

            results.append({
                "table": table_name,
                "input": input_path.removeprefix("s3://"),
                "output": f"{catalog_id}/{namespace}.{table_name}",
                "rowCount": count,
                "ingestDatetime": ingest_dt.isoformat(),
                "status": "succeeded",
            })
        except Exception as e:  # noqa: BLE001 — attempt every file, report per-file
            failed = True
            print(f"  FAILED {input_path} -> {iceberg_table}: {e}")
            results.append({
                "table": table_name,
                "input": input_path.removeprefix("s3://"),
                "status": "failed",
                "error": str(e),
            })

    if stats_s3_uri:
        _write_stats_file(spark, stats_s3_uri, results)

    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) < 4:
        print("Usage: load_data.py <batch_json> <catalog_id> <namespace> [stats_s3_uri]")
        sys.exit(1)
    stats_s3_uri = sys.argv[4] if len(sys.argv) > 4 else ""
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], stats_s3_uri))
