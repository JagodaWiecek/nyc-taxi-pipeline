"""Run the data pipeline: download raw files, load bronze, build silver.

Entry point of the Databricks job. The gold layer is built by dbt in the next job task.
"""

import logging

from pyspark.sql import SparkSession

from src.catalog_docs import (
    BRONZE_COLUMN_COMMENTS,
    BRONZE_TABLE_COMMENT,
    SILVER_COLUMN_COMMENTS,
    SILVER_TABLE_COMMENT,
    ZONES_BRONZE_COLUMN_COMMENTS,
    ZONES_BRONZE_TABLE_COMMENT,
    ZONES_SILVER_COLUMN_COMMENTS,
    ZONES_SILVER_TABLE_COMMENT,
    apply_comments,
)
from src.clients import TlcClient
from src.config import Config
from src.ingest import BronzeIngestor
from src.transform import SilverTransformer

logger = logging.getLogger(__name__)


def run(spark: SparkSession, config: Config) -> None:
    client = TlcClient(config)
    downloaded, missing = client.download_range(config.start_month, config.end_month)
    logger.info("Downloaded: %s. Not published yet: %s", downloaded, missing)
    client.download_zone_lookup()

    ingestor = BronzeIngestor(spark, config)
    ingestor.ingest_range(config.start_month, config.end_month)
    ingestor.ingest_zone_lookup()

    transformer = SilverTransformer(spark, config)
    transformer.transform_range(config.start_month, config.end_month)
    transformer.transform_zone_lookup()

    apply_comments(spark, config.bronze_table, BRONZE_TABLE_COMMENT, BRONZE_COLUMN_COMMENTS)
    apply_comments(spark, config.silver_table, SILVER_TABLE_COMMENT, SILVER_COLUMN_COMMENTS)
    apply_comments(
        spark, config.zone_lookup_table, ZONES_BRONZE_TABLE_COMMENT, ZONES_BRONZE_COLUMN_COMMENTS
    )
    apply_comments(
        spark, config.silver_zone_table, ZONES_SILVER_TABLE_COMMENT, ZONES_SILVER_COLUMN_COMMENTS
    )
    logger.info("Pipeline finished")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    spark = SparkSession.builder.getOrCreate()
    run(spark, Config())


if __name__ == "__main__":
    main()