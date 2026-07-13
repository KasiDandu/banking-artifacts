"""EventBridge (CloudTrail S3 data-event) handler.

Triggered by an EventBridge rule matching "AWS API Call via CloudTrail" PutObject /
CompleteMultipartUpload events against the raw landing bucket. Derives the source name from the
object key convention `raw/<source_name>/<file>`, resolves that source's config object in the
config bucket, and starts the Glue ETL job with the run's identifying parameters.

Bucket/job names are injected as Lambda environment variables by Terraform
(banking-infra/terraform/modules/banking-data/lambda.tf), not looked up via SSM at request time
-- Terraform already knows these values at deploy time, so a runtime SSM GetParameter call would
just add latency and an extra IAM permission for no benefit. The same values are also published
to SSM Parameter Store (ssm.tf) for other consumers/operational visibility.
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
RAW_KEY_PREFIX = os.environ.get("RAW_KEY_PREFIX", "raw/")
CONFIG_KEY_PREFIX = os.environ.get("CONFIG_KEY_PREFIX", "config/")


class UnrecognizedSourceError(Exception):
    pass


def _extract_bucket_and_key(event: dict[str, Any]) -> tuple[str, str]:
    detail = event["detail"]
    request_params = detail["requestParameters"]
    return request_params["bucketName"], request_params["key"]


def _derive_source_name(raw_key: str) -> str:
    if not raw_key.startswith(RAW_KEY_PREFIX):
        raise UnrecognizedSourceError(f"Key '{raw_key}' is outside raw prefix '{RAW_KEY_PREFIX}'")
    remainder = raw_key[len(RAW_KEY_PREFIX):]
    parts = remainder.split("/")
    if len(parts) < 2 or not parts[0]:
        raise UnrecognizedSourceError(f"Could not derive source_name from key '{raw_key}'")
    return parts[0]


def _config_key_for(source_name: str) -> str:
    return f"{CONFIG_KEY_PREFIX}{source_name}.json"


def _config_exists(config_key: str) -> bool:
    try:
        s3_client.head_object(Bucket=CONFIG_BUCKET, Key=config_key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("Received event: %s", event)

    raw_bucket, raw_key = _extract_bucket_and_key(event)

    try:
        source_name = _derive_source_name(raw_key)
    except UnrecognizedSourceError as exc:
        logger.warning("Skipping event, %s", exc)
        return {"status": "skipped", "reason": str(exc)}

    config_key = _config_key_for(source_name)
    if not _config_exists(config_key):
        logger.warning(
            "Skipping event, no config found for source '%s' at s3://%s/%s",
            source_name, CONFIG_BUCKET, config_key,
        )
        return {"status": "skipped", "reason": f"no config for source '{source_name}'"}

    run_id = str(uuid.uuid4())
    logger.info(
        "Starting Glue job '%s' run_id=%s for source='%s' raw=s3://%s/%s config=s3://%s/%s",
        GLUE_JOB_NAME, run_id, source_name, raw_bucket, raw_key, CONFIG_BUCKET, config_key,
    )

    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--RAW_BUCKET": raw_bucket,
            "--RAW_KEY": raw_key,
            "--CONFIG_BUCKET": CONFIG_BUCKET,
            "--CONFIG_KEY": config_key,
            "--RUN_ID": run_id,
        },
    )

    return {
        "status": "started",
        "source_name": source_name,
        "run_id": run_id,
        "glue_job_run_id": response["JobRunId"],
    }
