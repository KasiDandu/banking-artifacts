"""Create-or-refresh the Glue Crawler for a source's target Parquet path.

Ownership split with Terraform: the crawler *resource* is managed here, dynamically, per the
scenario's "create if not exists else refresh" requirement -- Terraform only provisions the
crawler's IAM role (see banking-infra/terraform/modules/banking-data/iam.tf) so Terraform and the
running Glue job never fight over the same AWS resource.
"""
from __future__ import annotations

from typing import Any


def ensure_crawler(
    glue_client, crawler_cfg: dict[str, Any], target_path: str, database: str, role_arn: str
) -> None:
    crawler_name = crawler_cfg["name"]
    targets = {"S3Targets": [{"Path": target_path}]}
    try:
        glue_client.get_crawler(Name=crawler_name)
        glue_client.update_crawler(Name=crawler_name, DatabaseName=database, Targets=targets)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database,
            Targets=targets,
            SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"},
        )
    glue_client.start_crawler(Name=crawler_name)
