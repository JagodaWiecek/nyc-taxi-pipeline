from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import os

from src.exceptions import IngestError
from src.clients import month_range
from src.config import Config
import logging

logger = logging.getLogger(__name__)

BRONZE_COLUMNS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp_ntz",
    "tpep_dropoff_datetime": "timestamp_ntz",
    "passenger_count": "long",
    "trip_distance": "double",
    "RatecodeID": "long",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "long",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "Airport_fee": "double",
}
class BronzeIngestor:

    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def read_month(self, year_month: str) -> DataFrame:
        """Read one raw Parquet file and force every column to the expected type.
        Some files have a column named 'airport_fee' instead of 'Airport_fee'.
        This function renames the column to the expected name.
        Also, some files have some variables 'long', and in another, the same column is 'double'.
        This function settles one common type for all columns.
        This function only builds the plan within pyspark, and does not execute the query.
        Args:
            year_month: YYYY-MM
            Returns:
                Spark DataFrame
        """
        path = self.config.raw_file_path(year_month)
        if not os.path.exists(path):
            raise IngestError(f"{year_month}: raw file not found: {path}")
        df = self.spark.read.parquet(path)

        for name in df.columns:
            if name.lower() == "airport_fee" and name != "Airport_fee":
                df = df.withColumnRenamed(name, "Airport_fee")

        missing = [c for c in BRONZE_COLUMNS if c not in df.columns]
        if missing:
            raise IngestError(f"{year_month}: columns missing in source file: {missing}")

        df = df.select(
            *[F.col(n).cast(t).alias(n) for n, t in BRONZE_COLUMNS.items()]
        )
        return (
            df.withColumn("_source_file", F.lit(path))
            .withColumn("_year_month", F.lit(year_month))
            .withColumn("_ingested_at", F.current_timestamp())
        )

    def _write(self, df: DataFrame, year_month: str) -> None:
        """
        Write the DataFrame to the bronze table.
        Args:
            df: Spark DataFrame
            year_month: YYYY-MM"""
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_year_month = '{year_month}'")
            .saveAsTable(self.config.bronze_table)
        )
    def is_loaded(self, year_month: str) -> bool:
        """This function verifies if the month is already loaded
        Args:
            year_month: YYYY-MM
        Returns:
            True if the month is already loaded, False otherwise
        """
        if not self.spark.catalog.tableExists(self.config.bronze_table):
            return False
        return (
            self.spark.table(self.config.bronze_table)
            .filter(F.col("_year_month") == year_month)
            .limit(1)
            .count()
            > 0
        )

    def ingest_month(self, year_month: str, force: bool = False) -> bool:
        """Load one month. Returns True if loaded, False if skipped.
        Args:
            year_month: YYYY-MM
            force: if True, force the load even if the month is already loaded
        Returns:
            True if loaded, False if skipped
        """
        if not force and self.is_loaded(year_month):
            logger.info("Already loaded, skipping: %s", year_month)
            return False
        try:
            self._write(self.read_month(year_month), year_month)
        except IngestError:
            raise
        except Exception as error:
            raise IngestError(
                f"{year_month}: failed to load into {self.config.bronze_table}: {error}"
            ) from error
        logger.info("Loaded: %s", year_month)
        return True

    def ingest_range(self, start: str, end: str, force: bool = False) -> list[str]:
        """Load every month in the range. Returns the months actually loaded.
        All months are attempted; if any failed, IngestError is raised at the end.
        Args:
            start: YYYY-MM
            end: YYYY-MM
            force: if True, force the load even if the month is already loaded
        Returns:
            list of months actually loaded
        """
        logger.info("Ingesting range %s to %s (force=%s)", start, end, force)
        loaded, failed = [], {}
        for year_month in month_range(start, end):
            try:
                if self.ingest_month(year_month, force=force):
                    loaded.append(year_month)
            except IngestError as error:
                logger.error("Failed: %s", error)
                failed[year_month] = str(error)
        if failed:
            raise IngestError(f"{len(failed)} month(s) failed: {failed}")
        return loaded