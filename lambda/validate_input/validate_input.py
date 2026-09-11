"""Validate Input: size-group already-enriched parquet files for EMR jobs.

By the time this runs (as the first Step Functions state), the Pipe's enrichment
Lambda has resolved each file's table name. Input is a FLAT list:

    [ { "SourceFile": <key>, "TableName": <table>, "Size": <bytes?> }, ... ]

`Size` is optional: when absent (e.g. a manual run, or an event without size)
it is backfilled with an S3 HeadObject. This step validates the shape, backfills
sizes, then groups files so each EMR job's inputs are sensible: any file larger
than BATCH_SIZE_THRESHOLD_BYTES gets its own group, and all remaining (small)
files share one group. One group becomes one EMR job and may span several tables.
"""
import json
import os

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.validation import SchemaValidationError, validate

logger = Logger()

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "input.schema.json")
with open(SCHEMA_PATH) as f:
    INPUT_SCHEMA = json.load(f)

# Files bigger than this size get their own emr job
BATCH_SIZE_THRESHOLD_BYTES = int(os.environ.get("BATCH_SIZE_THRESHOLD_BYTES"))

# Bucket holding the source parquet files, for HeadObject size backfill.
DATA_BUCKET = os.environ.get("DATA_BUCKET", "")

_s3 = boto3.client("s3")


def _backfill_sizes(files: list) -> None:
    """Fill in a missing/zero `Size` for each file via S3 HeadObject (in place)."""
    for f in files:
        if f.get("Size"):
            continue
        resp = _s3.head_object(Bucket=DATA_BUCKET, Key=f["SourceFile"])
        f["Size"] = int(resp["ContentLength"])
        logger.info("Backfilled size via HeadObject", extra={
            "source_file": f["SourceFile"], "size": f["Size"],
        })


def bin_pack(files: list, threshold_bytes: int) -> list:
    """Group files for EMR jobs: isolate big files, batch the rest together.

    Any file larger than `threshold_bytes` gets its own group (its own EMR job)
    so a large load never drags the small dailies' job. Every remaining file goes
    into a single shared group, so the per-job startup cost is paid once for all
    the small files. Table identity rides along per-file — one group (one EMR job)
    may write to several different Iceberg tables.

    Returns a list of groups: [{ "GroupIndex", "TotalSize", "TableCount",
    "SourceFileTableNameMapping": [{ "SourceFile", "TableName", "Size" }, ...] }].
    """
    def _item(f):
        return {"SourceFile": f["SourceFile"], "TableName": f["TableName"], "Size": f["Size"]}

    big = [[_item(f)] for f in files if f["Size"] > threshold_bytes]
    small = [_item(f) for f in files if f["Size"] <= threshold_bytes]

    raw_groups = big + ([small] if small else [])

    return [
        {
            "GroupIndex": i,
            "TotalSize": sum(m["Size"] for m in grp),
            "TableCount": len({m["TableName"] for m in grp}),
            "SourceFileTableNameMapping": grp,
        }
        for i, grp in enumerate(raw_groups)
    ]


@logger.inject_lambda_context
def handler(event: list, context: LambdaContext) -> dict:
    """
    - validate event schema
    - backfill file sizes if not provided (in case manually executed)
    - group parquets by threshold size
    """

    try:
        validate(event=event, schema=INPUT_SCHEMA)
    except SchemaValidationError as e:
        logger.error("Input validation failed", extra={"error": str(e)})
        raise

    files = event  # flat list of enriched records
    _backfill_sizes(files)
    groups = bin_pack(files, BATCH_SIZE_THRESHOLD_BYTES)

    logger.info("Bin-packed files into EMR job groups", extra={
        "file_count": len(files),
        "group_count": len(groups),
        "threshold_bytes": BATCH_SIZE_THRESHOLD_BYTES,
        "groups": [
            {"index": g["GroupIndex"], "files": len(g["SourceFileTableNameMapping"]),
             "bytes": g["TotalSize"], "tables": g["TableCount"]}
            for g in groups
        ],
    })

    return {"Groups": groups}
