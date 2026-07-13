import json

import pytest
from common.config_loader import ConfigError, parse_config

VALID_CONFIG = {
    "source_name": "customers",
    "raw_key_prefix": "raw/customers/",
    "file_format": "csv",
    "schema": [{"name": "customer_id", "dtype": "int", "nullable": False}],
    "target": {
        "database": "banking_data",
        "table": "customers",
        "s3_prefix": "processed/customers/",
        "load_mode": "upsert",
        "primary_keys": ["customer_id"],
        "version_column": "_ingested_at",
    },
    "crawler": {"name": "banking-data-customers-crawler"},
}


def test_parse_config_accepts_valid_document():
    config = parse_config(json.dumps(VALID_CONFIG))
    assert config["source_name"] == "customers"


def test_parse_config_rejects_invalid_json():
    with pytest.raises(ConfigError):
        parse_config("{not valid json")


def test_parse_config_rejects_missing_top_level_key():
    broken = {k: v for k, v in VALID_CONFIG.items() if k != "target"}
    with pytest.raises(ConfigError, match="target"):
        parse_config(json.dumps(broken))


def test_parse_config_rejects_unknown_load_mode():
    broken = json.loads(json.dumps(VALID_CONFIG))
    broken["target"]["load_mode"] = "bogus"
    with pytest.raises(ConfigError, match="load_mode"):
        parse_config(json.dumps(broken))


def test_parse_config_upsert_requires_primary_keys():
    broken = json.loads(json.dumps(VALID_CONFIG))
    broken["target"]["primary_keys"] = []
    with pytest.raises(ConfigError, match="primary_keys"):
        parse_config(json.dumps(broken))


def test_parse_config_upsert_requires_version_column():
    broken = json.loads(json.dumps(VALID_CONFIG))
    del broken["target"]["version_column"]
    with pytest.raises(ConfigError, match="version_column"):
        parse_config(json.dumps(broken))
