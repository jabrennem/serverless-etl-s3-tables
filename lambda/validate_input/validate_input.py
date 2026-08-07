from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.validation import SchemaValidationError, validate

import json
import os
import re

logger = Logger()

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "input.schema.json")
with open(SCHEMA_PATH) as f:
    INPUT_SCHEMA = json.load(f)

FILE_MAPPING_PATH = os.path.join(os.path.dirname(__file__), "file_mapping.json")
with open(FILE_MAPPING_PATH) as f:
    FILE_PATTERN_MAPPINGS = json.load(f)["mappings"]


def _compile_mappings(pattern_mappings: list) -> list:
    """Pre-compile regex patterns and sort by longest pattern (most specific first)."""
    compiled = []
    for pm in pattern_mappings:
        try:
            compiled.append({
                "tableName": pm["tableName"],
                "filePattern": pm["filePattern"],
                "regex": re.compile(pm["filePattern"]),
            })
        except re.error as e:
            raise ValueError(
                f"Invalid regex for table '{pm['tableName']}': {pm['filePattern']} — {e}"
            )
    return sorted(compiled, key=lambda pm: len(pm["filePattern"]), reverse=True)


def resolve_table_name(source_file: str, pattern_mappings: list) -> str:
    """Derive the table name from the source file name using registered regex patterns."""
    for pm in _compile_mappings(pattern_mappings):
        if pm["regex"].search(source_file):
            return pm["tableName"]
    raise ValueError(
        f"Unable to match source file '{source_file}' to a registered table. "
        f"Registered patterns: {[m['filePattern'] for m in pattern_mappings]}"
    )


@logger.inject_lambda_context
def handler(event: dict, context: LambdaContext) -> dict:
    # validate the event
    try:
        validate(event=event, schema=INPUT_SCHEMA)
    except SchemaValidationError as e:
        logger.error("Input validation failed", extra={"error": str(e)})
        raise

    # transform input for emr job
    for mapping in event["SourceFileTableNameMapping"]:
        if "TableName" not in mapping:
            table_name = resolve_table_name(mapping["SourceFile"], FILE_PATTERN_MAPPINGS)
            mapping["TableName"] = table_name
            logger.info("Derived TableName from pattern", extra={
                "source_file": mapping["SourceFile"], "table_name": table_name
            })
    logger.info("Input validation successful", extra={"mapping_count": len(event["SourceFileTableNameMapping"])})
    
    return event
