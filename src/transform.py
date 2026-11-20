import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.clients import month_range
from src.config import Config
from src.exceptions import SilverError
from src.ingest import BRONZE_COLUMNS

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Cleans the bronze table and writes the result to the silver table."""

    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def read_bronze_month(self, year_month: str) -> DataFrame:
        """Read one month from bronze. Raises SilverError if it is not there."""
        if not self.spark.catalog.tableExists(self.config.bronze_table):
            raise SilverError(f"Bronze table does not exist: {self.config.bronze_table}")
        df = self.spark.table(self.config.bronze_table).filter(
            f"_year_month = '{year_month}'"
        )
        if df.limit(1).count() == 0:
            raise SilverError(f"{year_month}: no rows in the bronze table")
        return df

    @staticmethod
    def clean(df: DataFrame) -> DataFrame:
        """Apply the silver rules, which are listed in docs/data_quality.md.
        Args:
            df: Spark DataFrame with columns as in BRONZE_COLUMNS.
        Returns:
            Spark DataFrame with columns as in BRONZE_COLUMNS, plus:
            - has_negative_amount: boolean
            - has_zero_distance: boolean
        """
        valid_time = (
            F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
        ) & (
            F.date_format("tpep_pickup_datetime", "yyyy-MM") == F.col("_year_month")
        )
        df = df.filter(valid_time)
        df = df.dropDuplicates(list(BRONZE_COLUMNS))

        df = df.withColumn(
            "passenger_count",
            F.when(F.col("passenger_count").between(1, 6), F.col("passenger_count")),
        )
        df = df.withColumn(
            "has_negative_amount",
            F.coalesce(
                (F.col("fare_amount") < 0) | (F.col("total_amount") < 0), F.lit(False)
            ),
        )
        df = df.withColumn(
            "has_zero_distance",
            F.coalesce(F.col("trip_distance") == 0, F.lit(False)),
        )
        return df.withColumn("_silver_processed_at", F.current_timestamp())
    
    def _write(self, df: DataFrame, year_month: str) -> None:
        """Write one month into the silver table, replacing only that month.
        Args:
            df: Spark DataFrame with columns as in BRONZE_COLUMNS, plus:
                - has_negative_amount: boolean
                - has_zero_distance: boolean
            year_month: YYYY-MM
        """
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_year_month = '{year_month}'")
            .saveAsTable(self.config.silver_table)
        )

    def is_loaded(self, year_month: str) -> bool:
        """Return True if the silver table already contains this month.
        Args:
            year_month: YYYY-MM
        Returns:
            True if the silver table already contains this month.
        """
        if not self.spark.catalog.tableExists(self.config.silver_table):
            return False
        return (
            self.spark.table(self.config.silver_table)
            .filter(f"_year_month = '{year_month}'")
            .limit(1)
            .count()
            > 0
        )

    def transform_month(self, year_month: str, force: bool = False) -> bool:
        """Clean one month from bronze into silver.
        Returns True if processed, False if skipped.
        Raises SilverError if the month cannot be processed.
        Args:
            year_month: YYYY-MM
            force: if True, process even if already in silver
        Returns:
            True if processed, False if skipped.
        """
        if not force and self.is_loaded(year_month):
            logger.info("Already in silver, skipping: %s", year_month)
            return False
        try:
            bronze = self.read_bronze_month(year_month)
            rows_before = bronze.count()
            self._write(self.clean(bronze), year_month)
            rows_after = (
                self.spark.table(self.config.silver_table)
                .filter(f"_year_month = '{year_month}'")
                .count()
            )
        except SilverError:
            raise
        except Exception as error:
            raise SilverError(f"{year_month}: transformation failed: {error}") from error
        logger.info(
            "Silver %s: %d rows in bronze, %d in silver (%d removed)",
            year_month, rows_before, rows_after, rows_before - rows_after,
        )
        return True

    def transform_range(self, start: str, end: str, force: bool = False) -> list[str]:
        """Process every month in the range. Returns the months actually processed.
        All months are attempted; if any failed, SilverError is raised at the end.
        Args:
            start: YYYY-MM
            end: YYYY-MM
            force: if True, process even if already in silver
        Returns:
            List of months actually processed.
        """
        logger.info("Transforming range %s to %s (force=%s)", start, end, force)
        done, failed = [], {}
        for year_month in month_range(start, end):
            try:
                if self.transform_month(year_month, force=force):
                    done.append(year_month)
            except SilverError as error:
                logger.error("Failed: %s", error)
                failed[year_month] = str(error)
        if failed:
            raise SilverError(f"{len(failed)} month(s) failed: {failed}")
        return done
    
    @staticmethod
    def clean_zones(df):
        """Rename columns, trim text, turn 'N/A' into NULL.
        Args:
            df: Spark DataFrame with columns as in ZONE_COLUMNS.
        Returns:
            Spark DataFrame with columns as in SILVER_ZONE_COLUMNS.
        """
        def text(name: str):
            trimmed = F.trim(F.col(name))
            return F.when(trimmed.isin("", "N/A"), F.lit(None)).otherwise(trimmed)

        return df.select(
            F.col("LocationID").alias("location_id"),
            text("Borough").alias("borough"),
            text("Zone").alias("zone"),
            text("service_zone").alias("service_zone"),
        )

    def transform_zone_lookup(self, force: bool = False) -> bool:
        """Bronze zones -> silver zones. Returns True if written, False if skipped."""
        target = self.config.silver_zone_table
        if not force and self.spark.catalog.tableExists(target):
            logger.info("Silver zones already loaded, skipping: %s", target)
            return False

        try:
            clean = self.clean_zones(self.spark.table(self.config.zone_lookup_table))
            total = clean.count()
            ids = clean.filter("location_id IS NOT NULL").select("location_id").distinct().count()
            if total == 0 or ids != total:
                raise SilverError(
                    f"zone lookup invalid: rows={total}, distinct non-null ids={ids}"
                )
            (
                clean.withColumn("_silver_processed_at", F.current_timestamp())
                .write.format("delta").mode("overwrite").saveAsTable(target)
            )
        except SilverError:
            raise
        except Exception as error:
            raise SilverError(f"zone lookup: failed to write {target}: {error}") from error

        logger.info("Loaded %d zones into %s", total, target)
        return True