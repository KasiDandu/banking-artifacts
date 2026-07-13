"""Build and persist the per-run lineage/audit record.

One `LineageRecord` is written per Glue job run to
`<lineage_base_path>/source=<name>/dt=<run_date>/run_id=<id>.parquet`, matching the Hive-style
partitioning of the fixed `banking_data.audit_lineage` Glue Catalog table (see
banking-infra/terraform/modules/banking-data/glue.tf) so it's queryable from Athena.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass
from typing import Any, Optional

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# Explicit schema so a single-row DataFrame never has to infer types -- several fields
# (rejected_s3_path, error_message, validation_error_summary, ...) are None on a normal run,
# and Spark can't infer a type from None alone with only one row.
_LINEAGE_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("raw_s3_bucket", StringType(), True),
        StructField("raw_s3_key", StringType(), True),
        StructField("config_s3_key", StringType(), True),
        StructField("load_mode", StringType(), True),
        StructField("target_s3_path", StringType(), True),
        StructField("target_database", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("glue_job_name", StringType(), True),
        StructField("glue_job_run_id", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("rows_read", LongType(), True),
        StructField("rows_valid", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("rejected_s3_path", StringType(), True),
        StructField("finished_at", StringType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("validation_error_summary", StringType(), True),
    ]
)


@dataclass
class LineageRecord:
    run_id: str
    source_name: str
    raw_s3_bucket: str
    raw_s3_key: str
    config_s3_key: str
    load_mode: str
    target_s3_path: str
    target_database: str
    target_table: str
    glue_job_name: str
    glue_job_run_id: str
    started_at: str
    rows_read: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    rejected_s3_path: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    status: str = "RUNNING"
    error_message: Optional[str] = None
    validation_error_summary: Optional[str] = None

    def mark_finished(self, status: str, error_message: Optional[str] = None) -> None:
        finished = dt.datetime.now(dt.timezone.utc)
        self.finished_at = finished.isoformat()
        self.status = status
        self.error_message = error_message
        started = dt.datetime.fromisoformat(self.started_at)
        self.duration_seconds = (finished - started).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_lineage_record(spark, record: LineageRecord, lineage_base_path: str, run_date: str) -> str:
    target_path = (
        f"{lineage_base_path.rstrip('/')}/source={record.source_name}"
        f"/dt={run_date}/run_id={record.run_id}"
    )
    record_dict = record.to_dict()
    row = tuple(record_dict.get(field.name) for field in _LINEAGE_SCHEMA.fields)
    df = spark.createDataFrame([row], schema=_LINEAGE_SCHEMA)
    df.write.mode("overwrite").parquet(target_path)
    return target_path
