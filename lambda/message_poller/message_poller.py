"""Scheduled message poller.

Invoked on a schedule by EventBridge Scheduler. Drains the ingest SQS queue; if
at least one message is present, resolves each file's table name and starts ONE
state machine execution with the flat file list, then deletes the drained
messages. If the queue is empty, it logs and returns without starting anything.
"""
import json
import os
import re

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

QUEUE_URL = os.environ["QUEUE_URL"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

FILE_MAPPING_PATH = os.path.join(os.path.dirname(__file__), "file_mapping.json")
with open(FILE_MAPPING_PATH) as f:
    FILE_PATTERN_MAPPINGS = json.load(f)["mappings"]

_COMPILED = sorted(
    ((m["tableName"], re.compile(m["filePattern"])) for m in FILE_PATTERN_MAPPINGS),
    key=lambda t: len(t[1].pattern),
    reverse=True,
)

_sqs = boto3.client("sqs")
_sfn = boto3.client("stepfunctions")


def _resolve_table(key: str) -> str:
    for name, rx in _COMPILED:
        if rx.search(key):
            return name
    raise ValueError(f"No table mapping matched '{key}'")


def _drain():
    """Receive all currently-available messages. Returns (files, receipt_handles)."""
    files, handles = [], []
    while True:
        resp = _sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            body = json.loads(m["Body"])
            obj = body.get("detail", {}).get("object", {})
            key = obj["key"]
            item = {"SourceFile": key, "TableName": _resolve_table(key)}
            if "size" in obj:
                item["Size"] = int(obj["size"])
            files.append(item)
            handles.append(m["ReceiptHandle"])
    return files, handles


def _delete(handles):
    for i in range(0, len(handles), 10):
        chunk = handles[i:i + 10]
        _sqs.delete_message_batch(
            QueueUrl=QUEUE_URL,
            Entries=[{"Id": str(j), "ReceiptHandle": h} for j, h in enumerate(chunk)],
        )


@logger.inject_lambda_context
def handler(event: dict, context: LambdaContext):
    """Poll for messages and launch the state machine."""
    files, handles = _drain()

    if not files:
        logger.info("No messages in queue; nothing to do.")
        return {"started": False, "fileCount": 0}

    logger.info("Draining queue and starting state machine", extra={
        "file_count": len(files),
        "tables": sorted({f["TableName"] for f in files}),
    })

    _sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(files),
    )

    _delete(handles)
    return {"started": True, "fileCount": len(files)}
