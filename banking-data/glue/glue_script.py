"""Glue Spark ETL job: config-driven validate -> transform -> load(mode) -> lineage -> crawler.

Deployed as the Glue job's script, with `banking-data/common/` shipped alongside it via the
job's `--extra-py-files` (see banking-infra/terraform/modules/banking-data/glue.tf), so
`common.config_loader` / `common.validation` / `common.load_modes` / `common.crawler` /
`common.lineage` are importable as-is.

Job arguments:
  Static (set as Glue job DefaultArguments by Terraform):
    --DATA_BUCKET       primary data-lake bucket (processed/, audit/, rejected/ prefixes)
    --CRAWLER_ROLE_ARN  IAM role the dynamically created/refreshed crawler assumes
  Per-run (passed by lambda_function.py at start_job_run time):
    --RAW_BUCKET --RAW_KEY --CONFIG_BUCKET --CONFIG_KEY --RUN_ID
"""
from __future__ import annotations

import datetime as dt
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from common.config_loader import parse_config
from common.crawler import ensure_crawler
from common.lineage import LineageRecord, write_lineage_record
from common.load_modes import apply_load_mode
from common.validation import validate_dataframe

ARGS = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATA_BUCKET",
        "CRAWLER_ROLE_ARN",
        "RAW_BUCKET",
        "RAW_KEY",
        "CONFIG_BUCKET",
        "CONFIG_KEY",
        "RUN_ID",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

DATA_BUCKET = ARGS["DATA_BUCKET"]
LINEAGE_BASE_PATH = f"s3://{DATA_BUCKET}/audit/lineage"
REJECTED_BASE_PATH = f"s3://{DATA_BUCKET}/rejected"


def load_config() -> dict:
    obj = s3_client.get_object(Bucket=ARGS["CONFIG_BUCKET"], Key=ARGS["CONFIG_KEY"])
    return parse_config(obj["Body"].read().decode("utf-8"))


def read_raw(config: dict):
    csv_options = config.get("csv_options", {})
    raw_path = f"s3://{ARGS['RAW_BUCKET']}/{ARGS['RAW_KEY']}"
    return (
        spark.read.option("header", csv_options.get("header", True))
        .option("delimiter", csv_options.get("delimiter", ","))
        .csv(raw_path)
    )


def main() -> None:
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    run_id = ARGS["RUN_ID"]

    # glue_job_run_id defaults to our own run_id: a job started via start_job_run has no
    # built-in way to read back its own JobRunId from inside the script, so RUN_ID (generated
    # by lambda_function.py and passed as a job argument) is the join key across lineage rows,
    # CloudWatch logs, and the Glue job-run history.
    record = LineageRecord(
        run_id=run_id,
        source_name="unknown",
        raw_s3_bucket=ARGS["RAW_BUCKET"],
        raw_s3_key=ARGS["RAW_KEY"],
        config_s3_key=ARGS["CONFIG_KEY"],
        load_mode="unknown",
        target_s3_path="",
        target_database="",
        target_table="",
        glue_job_name=ARGS["JOB_NAME"],
        glue_job_run_id=run_id,
        started_at=started_at,
    )
    run_date = started_at[:10]

    try:
        config = load_config()
        record.source_name = config["source_name"]
        record.load_mode = config["target"]["load_mode"]
        record.target_database = config["target"]["database"]
        record.target_table = config["target"]["table"]

        raw_df = read_raw(config)
        record.rows_read = raw_df.count()

        valid_df, rejected_df = validate_dataframe(raw_df, config["schema"])
        record.rows_valid = valid_df.count()
        record.rows_rejected = record.rows_read - record.rows_valid

        if record.rows_rejected > 0:
            rejected_path = (
                f"{REJECTED_BASE_PATH}/{config['source_name']}/dt={run_date}/run_id={run_id}"
            )
            rejected_df.withColumn("_run_id", F.lit(run_id)).write.mode("overwrite").parquet(
                rejected_path
            )
            record.rejected_s3_path = rejected_path
            error_counts = (
                rejected_df.select(F.explode("_validation_errors").alias("error"))
                .groupBy("error")
                .count()
                .orderBy(F.desc("count"))
                .limit(20)
                .collect()
            )
            record.validation_error_summary = str(
                [{"error": row["error"], "count": row["count"]} for row in error_counts]
            )

        target_path = apply_load_mode(
            spark, valid_df, config["target"], run_id, f"s3://{DATA_BUCKET}"
        )
        record.target_s3_path = target_path

        ensure_crawler(
            glue_client,
            config["crawler"],
            target_path,
            config["target"]["database"],
            ARGS["CRAWLER_ROLE_ARN"],
        )

        record.mark_finished("SUCCEEDED")
    except Exception as exc:
        record.mark_finished("FAILED", error_message=str(exc))
        write_lineage_record(spark, record, LINEAGE_BASE_PATH, run_date)
        job.commit()
        raise

    write_lineage_record(spark, record, LINEAGE_BASE_PATH, run_date)
    job.commit()


if __name__ == "__main__":
    main()
