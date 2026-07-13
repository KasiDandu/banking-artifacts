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
    df = spark.createDataFrame([record.to_dict()])
    df.write.mode("overwrite").parquet(target_path)
    return target_path
