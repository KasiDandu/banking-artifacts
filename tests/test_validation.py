import pytest
from common.validation import validate_dataframe

SCHEMA = [
    {"name": "account_id", "dtype": "int", "nullable": False},
    {
        "name": "account_type",
        "dtype": "string",
        "nullable": False,
        "possible_values": ["Checking", "Savings", "Credit"],
    },
    {"name": "balance", "dtype": "float", "nullable": False},
]


def _raw_df(spark, rows):
    return spark.createDataFrame(rows, ["account_id", "account_type", "balance"])


def test_valid_rows_pass_through(spark):
    raw_df = _raw_df(spark, [("1", "Checking", "100.50")])
    valid_df, rejected_df = validate_dataframe(raw_df, SCHEMA)

    assert valid_df.count() == 1
    assert rejected_df.count() == 0
    row = valid_df.collect()[0]
    assert row["account_id"] == 1
    assert row["balance"] == 100.5


def test_bad_dtype_is_rejected(spark):
    raw_df = _raw_df(spark, [("not-an-int", "Checking", "100.50")])
    valid_df, rejected_df = validate_dataframe(raw_df, SCHEMA)

    assert valid_df.count() == 0
    assert rejected_df.count() == 1
    errors = rejected_df.collect()[0]["_validation_errors"]
    assert any("cannot cast" in e for e in errors)


def test_null_in_non_nullable_column_is_rejected(spark):
    raw_df = _raw_df(spark, [(None, "Checking", "100.50")])
    valid_df, rejected_df = validate_dataframe(raw_df, SCHEMA)

    assert valid_df.count() == 0
    errors = rejected_df.collect()[0]["_validation_errors"]
    assert any("null value not allowed" in e for e in errors)


def test_value_outside_allowed_set_is_rejected(spark):
    raw_df = _raw_df(spark, [("1", "Bitcoin", "100.50")])
    valid_df, rejected_df = validate_dataframe(raw_df, SCHEMA)

    assert valid_df.count() == 0
    errors = rejected_df.collect()[0]["_validation_errors"]
    assert any("not in allowed set" in e for e in errors)


def test_multiple_violations_are_all_captured(spark):
    raw_df = _raw_df(spark, [(None, "Bitcoin", "100.50")])
    _, rejected_df = validate_dataframe(raw_df, SCHEMA)

    errors = rejected_df.collect()[0]["_validation_errors"]
    assert len(errors) == 2


def test_missing_configured_column_raises(spark):
    raw_df = spark.createDataFrame([("1",)], ["account_id"])
    with pytest.raises(ValueError, match="missing configured columns"):
        validate_dataframe(raw_df, SCHEMA)
