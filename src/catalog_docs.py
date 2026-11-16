import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

BRONZE_TABLE_COMMENT = (
    "Raw NYC TLC yellow taxi trips, loaded as received (types unified). "
    "Invalid records are intentionally kept."
)

BRONZE_COLUMN_COMMENTS = {
    "VendorID": "Code of the technology provider that supplied the trip record.",
    "tpep_pickup_datetime": "Date and time when the meter was engaged.",
    "tpep_dropoff_datetime": "Date and time when the meter was disengaged.",
    "passenger_count": "Number of passengers, entered manually by the driver.",
    "trip_distance": "Trip distance in miles reported by the taximeter.",
    "RatecodeID": "Rate code in effect at the end of the trip.",
    "store_and_fwd_flag": "Y if the record was stored in the vehicle before being sent to the vendor, N otherwise.",
    "PULocationID": "TLC taxi zone where the meter was engaged (pickup).",
    "DOLocationID": "TLC taxi zone where the meter was disengaged (dropoff).",
    "payment_type": "How the passenger paid (see TLC data dictionary).",
    "fare_amount": "Time-and-distance fare calculated by the meter, in USD.",
    "extra": "Miscellaneous extras and surcharges, in USD.",
    "mta_tax": "MTA tax triggered by the metered rate, in USD.",
    "tip_amount": "Tip in USD. Cash tips are not recorded.",
    "tolls_amount": "Total tolls paid during the trip, in USD.",
    "improvement_surcharge": "Improvement surcharge assessed at the start of the trip, in USD.",
    "total_amount": "Total amount charged to the passenger in USD. Excludes cash tips.",
    "congestion_surcharge": "New York State congestion surcharge collected, in USD.",
    "Airport_fee": "Fee for pickups at LaGuardia and JFK airports, in USD.",
    "_source_file": "Path of the raw Parquet file the row came from.",
    "_year_month": "Month of the source file (YYYY-MM).",
    "_ingested_at": "When the row was loaded into the bronze table.",
}

SILVER_TABLE_COMMENT = (
    "Cleaned NYC TLC yellow taxi trips: impossible durations and out-of-month "
    "pickups dropped, duplicates removed, quality flags added."
)

SILVER_COLUMN_COMMENTS = {
    **BRONZE_COLUMN_COMMENTS,
    "passenger_count": "Number of passengers (1-6). Null if missing or outside 1-6 in the source.",
    "has_negative_amount": "True if fare_amount or total_amount is negative.",
    "has_zero_distance": "True if trip_distance is 0.",
    "_silver_processed_at": "When the row was written to the silver table.",
}


def _quote(text: str) -> str:
    """Escape single quotes so the text can be put inside a SQL string."""
    return text.replace("'", "''")


def apply_comments(
    spark: SparkSession,
    table: str,
    table_comment: str,
    column_comments: dict[str, str],
) -> None:
    """Set the table comment and a comment on every listed column that exists."""
    spark.sql(f"COMMENT ON TABLE {table} IS '{_quote(table_comment)}'")
    existing = set(spark.table(table).columns)
    for column, comment in column_comments.items():
        if column in existing:
            spark.sql(f"COMMENT ON COLUMN {table}.`{column}` IS '{_quote(comment)}'")
        else:
            logger.warning("No column %s in %s, comment skipped", column, table)
    logger.info("Comments applied to %s", table)