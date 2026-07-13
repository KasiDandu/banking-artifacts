from unittest.mock import MagicMock

from common.crawler import ensure_crawler


class _EntityNotFoundException(Exception):
    pass


def _make_glue_client():
    client = MagicMock()
    client.exceptions.EntityNotFoundException = _EntityNotFoundException
    return client


def test_ensure_crawler_creates_when_missing():
    client = _make_glue_client()
    client.get_crawler.side_effect = _EntityNotFoundException("not found")

    ensure_crawler(
        client,
        {"name": "customers-crawler"},
        target_path="s3://bucket/processed/customers/",
        database="banking_data",
        role_arn="arn:aws:iam::123456789012:role/crawler-role",
    )

    client.create_crawler.assert_called_once()
    client.update_crawler.assert_not_called()
    client.start_crawler.assert_called_once_with(Name="customers-crawler")


def test_ensure_crawler_refreshes_when_present():
    client = _make_glue_client()
    client.get_crawler.return_value = {"Crawler": {"Name": "customers-crawler"}}

    ensure_crawler(
        client,
        {"name": "customers-crawler"},
        target_path="s3://bucket/processed/customers/",
        database="banking_data",
        role_arn="arn:aws:iam::123456789012:role/crawler-role",
    )

    client.update_crawler.assert_called_once()
    client.create_crawler.assert_not_called()
    client.start_crawler.assert_called_once_with(Name="customers-crawler")
