"""Config-driven schema/dtype/nullability/allowed-value validation for Spark DataFrames.

`validate_dataframe` takes a raw, all-string DataFrame (as read from CSV) and a source config's
`schema` list, and returns a (valid_df, rejected_df) pair: `valid_df` has every column cast to
its configured dtype, `rejected_df` keeps the original raw string columns plus a
`_validation_errors` array column describing every rule that failed for that row.
"""
from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

_DTYPE_MAP: dict[str, T.DataType] = {
    "int": T.IntegerType(),
    "long": T.LongType(),
    "float": T.DoubleType(),
    "double": T.DoubleType(),
    "string": T.StringType(),
    "date": T.DateType(),
    "boolean": T.BooleanType(),
}


def spark_type_for(dtype: str) -> T.DataType:
    try:
        return _DTYPE_MAP[dtype]
    except KeyError as exc:
        raise ValueError(f"Unsupported dtype '{dtype}'; supported: {sorted(_DTYPE_MAP)}") from exc


def validate_dataframe(raw_df: DataFrame, schema: list[dict[str, Any]]) -> tuple[DataFrame, DataFrame]:
    missing = [col["name"] for col in schema if col["name"] not in raw_df.columns]
    if missing:
        raise ValueError(f"Raw data is missing configured columns: {missing}")

    df = raw_df
    error_columns: list[Column] = []

    for col_cfg in schema:
        name = col_cfg["name"]
        dtype = col_cfg["dtype"]
        nullable = col_cfg.get("nullable", True)
        possible_values = col_cfg.get("possible_values")

        raw_col = F.col(name)
        is_blank = raw_col.isNull() | (F.trim(raw_col) == F.lit(""))
        cast_col = F.trim(raw_col).cast(spark_type_for(dtype))
        cast_failed = (~is_blank) & cast_col.isNull()

        df = df.withColumn(f"_cast_{name}", cast_col)

        error_columns.append(F.when(cast_failed, F.lit(f"{name}: cannot cast value to {dtype}")))
        if not nullable:
            error_columns.append(F.when(is_blank, F.lit(f"{name}: null value not allowed")))
        if possible_values:
            not_allowed = (~is_blank) & (~cast_failed) & (~cast_col.isin(*possible_values))
            error_columns.append(
                F.when(not_allowed, F.lit(f"{name}: value not in allowed set {possible_values}"))
            )

    df = df.withColumn(
        "_validation_errors",
        F.array_except(F.array(*error_columns), F.array(F.lit(None).cast("string"))),
    )

    valid_df = df.filter(F.size("_validation_errors") == 0).select(
        *[F.col(f"_cast_{c['name']}").alias(c["name"]) for c in schema]
    )
    rejected_df = df.filter(F.size("_validation_errors") > 0).select(
        *[c["name"] for c in schema], "_validation_errors"
    )
    return valid_df, rejected_df
