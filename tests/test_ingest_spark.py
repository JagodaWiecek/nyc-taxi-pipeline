"""Tests of reading raw files into the bronze schema on a real Spark session (need pyspark and Java)."""

from src.ingest import BRONZE_COLUMNS, BronzeIngestor


class FakeConfig:
    """Points raw_file_path() at a temporary folder instead of the Volume."""

    def __init__(self, folder):
        self.folder = folder

    def raw_file_path(self, year_month):
        return str(self.folder / f"yellow_tripdata_{year_month}.parquet")

def _write_source_file(spark, folder):
    """Write a one-row Parquet file that looks like a messy source file:
    a lowercase 'airport_fee' column and passenger_count stored as double."""
    spark.range(1).selectExpr(
        "1 AS VendorID",
        "timestamp'2024-01-01 10:00:00' AS tpep_pickup_datetime",
        "timestamp'2024-01-01 10:20:00' AS tpep_dropoff_datetime",
        "CAST(2 AS DOUBLE) AS passenger_count",
        "3.5 AS trip_distance",
        "CAST(1 AS DOUBLE) AS RatecodeID",
        "'N' AS store_and_fwd_flag",
        "100 AS PULocationID",
        "200 AS DOLocationID",
        "CAST(1 AS DOUBLE) AS payment_type",
        "15.0 AS fare_amount",
        "0.5 AS extra",
        "0.5 AS mta_tax",
        "3.0 AS tip_amount",
        "0.0 AS tolls_amount",
        "1.0 AS improvement_surcharge",
        "20.0 AS total_amount",
        "2.5 AS congestion_surcharge",
        "1.75 AS airport_fee",
    ).write.parquet(str(folder / "yellow_tripdata_2024-01.parquet"))


def test_read_month_unifies_names_and_types(spark, tmp_path):
    """Columns get the expected names and types, whatever the source used."""
    _write_source_file(spark, tmp_path)
    ingestor = BronzeIngestor(spark, FakeConfig(tmp_path))

    df = ingestor.read_month("2024-01")

    types = dict(df.dtypes)
    assert "Airport_fee" in types and "airport_fee" not in types
    assert types["passenger_count"] == "bigint"
    assert types["payment_type"] == "bigint"
    assert types["tpep_pickup_datetime"] == "timestamp_ntz"


def test_read_month_adds_technical_columns_at_the_end(spark, tmp_path):
    """Source columns come first, then the three technical columns."""
    _write_source_file(spark, tmp_path)
    ingestor = BronzeIngestor(spark, FakeConfig(tmp_path))

    df = ingestor.read_month("2024-01")

    expected = list(BRONZE_COLUMNS) + ["_source_file", "_year_month", "_ingested_at"]
    assert df.columns == expected
    assert df.first()["_year_month"] == "2024-01"







