import time

from common.load_modes import apply_load_mode


def _target(load_mode, s3_prefix, **overrides):
    target = {
        "load_mode": load_mode,
        "s3_prefix": s3_prefix,
        "primary_keys": ["id"],
        "version_column": "_ingested_at",
    }
    target.update(overrides)
    return target


def test_truncate_load_overwrites_entirely(spark, tmp_path):
    base_uri = tmp_path.as_uri()
    target = _target("truncate_load", "truncate_case/")

    df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    apply_load_mode(spark, df1, target, "run-1", base_uri)

    df2 = spark.createDataFrame([(3, "Carol")], ["id", "name"])
    target_path = apply_load_mode(spark, df2, target, "run-2", base_uri)

    result = spark.read.parquet(target_path).collect()
    assert {row["id"] for row in result} == {3}


def test_append_keeps_all_rows(spark, tmp_path):
    base_uri = tmp_path.as_uri()
    target = _target("append", "append_case/")

    df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    apply_load_mode(spark, df1, target, "run-1", base_uri)

    df2 = spark.createDataFrame([(2, "Bob")], ["id", "name"])
    target_path = apply_load_mode(spark, df2, target, "run-2", base_uri)

    result = spark.read.parquet(target_path).collect()
    assert {row["id"] for row in result} == {1, 2}


def test_upsert_keeps_latest_by_version_column(spark, tmp_path):
    base_uri = tmp_path.as_uri()
    target = _target("upsert", "upsert_case/")

    df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    apply_load_mode(spark, df1, target, "run-1", base_uri)

    time.sleep(0.05)

    df2 = spark.createDataFrame([(1, "Alice Updated"), (3, "Carol")], ["id", "name"])
    target_path = apply_load_mode(spark, df2, target, "run-2", base_uri)

    result = {row["id"]: row["name"] for row in spark.read.parquet(target_path).collect()}
    assert result == {1: "Alice Updated", 2: "Bob", 3: "Carol"}


def test_upsert_first_load_has_no_prior_data(spark, tmp_path):
    base_uri = tmp_path.as_uri()
    target = _target("upsert", "upsert_first_case/")

    df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    target_path = apply_load_mode(spark, df1, target, "run-1", base_uri)

    result = spark.read.parquet(target_path).collect()
    assert len(result) == 1
