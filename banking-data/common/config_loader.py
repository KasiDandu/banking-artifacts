"""Load and structurally validate a per-source config JSON (schema/target/crawler blocks).

This validates the *shape* of the config document itself (missing keys, unknown load_mode,
etc.) so a malformed config fails fast in the Glue job with a clear error, before any Spark
DataFrame work happens.
"""
from __future__ import annotations

import json
from typing import Any

_REQUIRED_TOP_LEVEL = ["source_name", "raw_key_prefix", "file_format", "schema", "target", "crawler"]
_REQUIRED_SCHEMA_FIELDS = ["name", "dtype"]
_REQUIRED_TARGET_FIELDS = ["database", "table", "s3_prefix", "load_mode", "primary_keys"]
_VALID_LOAD_MODES = {"truncate_load", "append", "upsert"}


class ConfigError(ValueError):
    pass


def parse_config(raw_json: str) -> dict[str, Any]:
    try:
        config = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Config is not valid JSON: {exc}") from exc

    missing_top = [k for k in _REQUIRED_TOP_LEVEL if k not in config]
    if missing_top:
        raise ConfigError(f"Config missing required top-level keys: {missing_top}")

    if not config["schema"]:
        raise ConfigError("Config 'schema' must list at least one column")
    for col in config["schema"]:
        missing_col = [k for k in _REQUIRED_SCHEMA_FIELDS if k not in col]
        if missing_col:
            raise ConfigError(f"Schema entry {col} missing required keys: {missing_col}")

    target = config["target"]
    missing_target = [k for k in _REQUIRED_TARGET_FIELDS if k not in target]
    if missing_target:
        raise ConfigError(f"Config 'target' missing required keys: {missing_target}")

    load_mode = target["load_mode"]
    if load_mode not in _VALID_LOAD_MODES:
        raise ConfigError(f"target.load_mode must be one of {sorted(_VALID_LOAD_MODES)}, got {load_mode!r}")

    if load_mode == "upsert":
        if not target.get("primary_keys"):
            raise ConfigError("target.load_mode 'upsert' requires a non-empty target.primary_keys")
        if not target.get("version_column"):
            raise ConfigError("target.load_mode 'upsert' requires target.version_column")

    if "name" not in config["crawler"]:
        raise ConfigError("Config 'crawler' must include 'name'")

    return config


def load_config_from_path(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return parse_config(fh.read())
