"""Apply a source's configured load_mode (truncate_load / append / upsert) to validated data.

`base_uri` is the scheme+bucket portion of the target path (e.g. `s3://<data-bucket>` in
production, or a local/tmp URI in tests) so this is testable against the local filesystem
without touching S3.
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_load_mode(
    spark, valid_df: DataFrame, target: dict[str, Any], run_id: str, base_uri: str
) -> str:
    load_mode = target["load_mode"]
    target_path = f"{base_uri.rstrip('/')}/{target['s3_prefix'].strip('/')}"

    stamped_df = valid_df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_run_id", F.lit(run_id)
    )

    if load_mode == "truncate_load":
        stamped_df.write.mode("overwrite").parquet(target_path)
    elif load_mode == "append":
        stamped_df.write.mode("append").parquet(target_path)
    elif load_mode == "upsert":
        primary_keys = target["primary_keys"]
        version_column = target["version_column"]
        try:
            existing_df = spark.read.parquet(target_path)
            combined_df = existing_df.unionByName(stamped_df, allowMissingColumns=True)
        except Exception:
            # No prior data at target_path yet -- first-ever load for this source.
            combined_df = stamped_df

        window = Window.partitionBy(*primary_keys).orderBy(F.col(version_column).desc())
        deduped_df = (
            combined_df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        deduped_df.write.mode("overwrite").parquet(target_path)
    else:
        raise ValueError(f"Unsupported load_mode '{load_mode}'")

    return target_path
