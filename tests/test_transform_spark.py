"""Tests of the silver cleaning rules on a real Spark session (need pyspark and Java)."""

from datetime import datetime

from src.ingest import BRONZE_COLUMNS
from src.transform import SilverTransformer

SCHEMA = ", ".join(f"{name} {dtype}" for name, dtype in BRONZE_COLUMNS.items())
SCHEMA += ", _year_month string"
MAX_MILES = 100.0


def _clean(df):
    """Run the cleaning rules with the test distance threshold."""
    return SilverTransformer.clean(df, MAX_MILES)


def _trip(**overrides):
    """One valid trip. Each test changes only the fields it cares about."""
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 10, 10, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 10, 10, 20, 0),
        "passenger_count": 1,
        "trip_distance": 3.0,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 100,
        "DOLocationID": 200,
        "payment_type": 1,
        "fare_amount": 15.0,
        "extra": 0.5,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 20.0,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "_year_month": "2024-01",
    }
    row.update(overrides)
    return row


def _make_df(spark, *rows):
    """Turn trip dictionaries into a Spark DataFrame with the bronze schema."""
    columns = list(BRONZE_COLUMNS) + ["_year_month"]
    data = [tuple(row[column] for column in columns) for row in rows]
    return spark.createDataFrame(data, schema=SCHEMA)


def test_clean_drops_impossible_durations(spark):
    """Dropoff before or at pickup time is removed."""
    pickup = datetime(2024, 1, 10, 10, 0, 0)
    df = _make_df(
        spark,
        _trip(trip_distance=1.0),
        _trip(trip_distance=2.0, tpep_dropoff_datetime=datetime(2024, 1, 10, 9, 0, 0)),
        _trip(trip_distance=3.0, tpep_dropoff_datetime=pickup),
    )

    result = _clean(df).collect()

    assert [row["trip_distance"] for row in result] == [1.0]


def test_clean_drops_pickup_outside_the_files_month(spark):
    """A trip whose pickup date is not in the file's month is removed."""
    df = _make_df(
        spark,
        _trip(trip_distance=1.0),
        _trip(
            trip_distance=2.0,
            tpep_pickup_datetime=datetime(2024, 2, 1, 10, 0, 0),
            tpep_dropoff_datetime=datetime(2024, 2, 1, 10, 20, 0),
        ),
    )

    result = _clean(df).collect()

    assert [row["trip_distance"] for row in result] == [1.0]


def test_clean_sets_passenger_count_outside_1_to_6_to_null(spark):
    """Only 1-6 passengers are kept; 0, 7+ and missing values become null."""
    df = _make_df(
        spark,
        _trip(trip_distance=1.0, passenger_count=0),
        _trip(trip_distance=2.0, passenger_count=7),
        _trip(trip_distance=3.0, passenger_count=None),
        _trip(trip_distance=4.0, passenger_count=3),
    )

    result = {row["trip_distance"]: row["passenger_count"] for row in _clean(df).collect()}

    assert result == {1.0: None, 2.0: None, 3.0: None, 4.0: 3}


def test_clean_flags_negative_amounts_without_dropping_rows(spark):
    """Negative amounts are flagged, and a missing amount is not flagged."""
    df = _make_df(
        spark,
        _trip(trip_distance=1.0),
        _trip(trip_distance=2.0, fare_amount=-5.0),
        _trip(trip_distance=3.0, total_amount=-3.0),
        _trip(trip_distance=4.0, total_amount=None),
    )

    result = {row["trip_distance"]: row["has_negative_amount"] for row in _clean(df).collect()}

    assert result == {1.0: False, 2.0: True, 3.0: True, 4.0: False}


def test_clean_flags_zero_distance_without_dropping_rows(spark):
    """Zero-distance trips are kept and flagged."""
    df = _make_df(spark, _trip(trip_distance=0.0), _trip(trip_distance=5.0))

    result = {row["trip_distance"]: row["has_zero_distance"] for row in _clean(df).collect()}

    assert result == {0.0: True, 5.0: False}


def test_clean_flags_outlier_distance_above_the_threshold(spark):
    """Only distances strictly above the threshold are flagged; the rows are kept."""
    df = _make_df(
        spark,
        _trip(trip_distance=3.0),
        _trip(trip_distance=MAX_MILES),
        _trip(trip_distance=MAX_MILES + 0.5),
    )

    result = {row["trip_distance"]: row["has_outlier_distance"] for row in _clean(df).collect()}

    assert result == {3.0: False, MAX_MILES: False, MAX_MILES + 0.5: True}


def test_clean_removes_only_rows_identical_in_all_source_columns(spark):
    """Exact copies collapse to one row; a trip differing in one column stays."""
    df = _make_df(spark, _trip(), _trip(), _trip(tip_amount=9.0))

    assert _clean(df).count() == 2


def test_clean_adds_the_expected_new_columns(spark):
    """clean adds three flags and a processing timestamp, and keeps all other columns."""
    df = _make_df(spark, _trip())

    result = _clean(df)

    new_columns = set(result.columns) - set(df.columns)
    assert new_columns == {
        "has_negative_amount",
        "has_zero_distance",
        "has_outlier_distance",
        "_silver_processed_at",
    }
    assert result.first()["_silver_processed_at"] is not None