from pathlib import Path

from common.config_loader import parse_config
from common.load_modes import apply_load_mode
from common.validation import validate_dataframe

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_customers_pipeline_end_to_end(spark, tmp_path):
    config = parse_config((FIXTURES_DIR / "customers_config.json").read_text())

    raw_df = spark.read.option("header", True).csv(str(FIXTURES_DIR / "customers_sample.csv"))

    valid_df, rejected_df = validate_dataframe(raw_df, config["schema"])
    # 2 valid rows; "bad" customer_id fails the int cast, blank first_name fails nullability.
    assert valid_df.count() == 2
    assert rejected_df.count() == 2

    target_path = apply_load_mode(spark, valid_df, config["target"], "run-e2e", tmp_path.as_uri())

    result = spark.read.parquet(target_path).collect()
    assert {row["customer_id"] for row in result} == {1, 2}
