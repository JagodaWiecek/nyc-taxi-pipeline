from pyspark.sql import DataFrame, SparkSession

from src.config import Config

"""
The code is supposed to answer the set questions.
It is best to run the "run_all" function in the notebook or main file to see the results
"""
def rows_per_month(spark: SparkSession, config: Config) -> DataFrame:
    """Question: how many rows did each monthly file contribute?"""
    return spark.sql(f"""
        SELECT _year_month, COUNT(*) AS rows
        FROM {config.bronze_table}
        GROUP BY _year_month
        ORDER BY _year_month
    """)


def suspicious_records(spark: SparkSession, config: Config) -> DataFrame:
    """Question: how many rows look suspicious, by type of problem?

    One row of results. A single trip can fall into several categories,
    so the counts must not be added together.
    """
    return spark.sql(f"""
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS negative_fare,
          SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total,
          SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) AS zero_distance,
          SUM(CASE WHEN tpep_dropoff_datetime < tpep_pickup_datetime THEN 1 ELSE 0 END) AS dropoff_before_pickup,
          SUM(CASE WHEN tpep_dropoff_datetime = tpep_pickup_datetime THEN 1 ELSE 0 END) AS zero_duration,
          SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS null_passengers,
          SUM(CASE WHEN date_format(tpep_pickup_datetime, 'yyyy-MM') <> _year_month THEN 1 ELSE 0 END) AS pickup_outside_month
        FROM {config.bronze_table}
    """)


def passenger_count_distribution(spark: SparkSession, config: Config) -> DataFrame:
    """Question: which passenger counts occur, and how often?"""
    return spark.sql(f"""
        SELECT passenger_count, COUNT(*) AS rows
        FROM {config.bronze_table}
        GROUP BY passenger_count
        ORDER BY passenger_count
    """)


def negative_fare_by_payment_type(spark: SparkSession, config: Config) -> DataFrame:
    """Question: which payment types do the negative fares belong to?"""
    return spark.sql(f"""
        SELECT payment_type, COUNT(*) AS rows
        FROM {config.bronze_table}
        WHERE fare_amount < 0
        GROUP BY payment_type
        ORDER BY rows DESC
    """)


def nulls_by_vendor(spark: SparkSession, config: Config) -> DataFrame:
    """Question: are missing values concentrated at one vendor, and do they go missing together?"""
    return spark.sql(f"""
        SELECT
          VendorID,
          COUNT(*) AS total,
          SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS null_passengers,
          SUM(CASE WHEN RatecodeID IS NULL THEN 1 ELSE 0 END) AS null_ratecode,
          SUM(CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END) AS null_congestion,
          SUM(CASE WHEN Airport_fee IS NULL THEN 1 ELSE 0 END) AS null_airport_fee
        FROM {config.bronze_table}
        GROUP BY VendorID
        ORDER BY VendorID
    """)


def nulls_by_payment_type(spark: SparkSession, config: Config) -> DataFrame:
    """Question: are missing passenger counts tied to a payment type?"""
    return spark.sql(f"""
        SELECT
          payment_type,
          COUNT(*) AS total,
          SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS null_passengers
        FROM {config.bronze_table}
        GROUP BY payment_type
        ORDER BY payment_type
    """)


def zero_distance_context(spark: SparkSession, config: Config) -> DataFrame:
    """Question: are trips with zero distance paid, and do they stay in one zone?"""
    return spark.sql(f"""
        SELECT
          SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) AS zero_distance,
          SUM(CASE WHEN trip_distance = 0 AND fare_amount > 0 THEN 1 ELSE 0 END) AS zero_distance_paid,
          SUM(CASE WHEN trip_distance = 0 AND PULocationID = DOLocationID THEN 1 ELSE 0 END) AS zero_distance_same_zone
        FROM {config.bronze_table}
    """)


# Every analysis in the order it appears in docs/data_quality.md
ANALYSES = [
    rows_per_month,
    suspicious_records,
    passenger_count_distribution,
    negative_fare_by_payment_type,
    nulls_by_vendor,
    nulls_by_payment_type,
    zero_distance_context,
]


def run_all(spark: SparkSession, config: Config) -> None:
    """Print the title and the result table of every analysis."""
    for analysis in ANALYSES:
        print(f"\n=== {analysis.__name__} ===")
        print(analysis.__doc__.strip().splitlines()[0])
        analysis(spark, config).show(50, truncate=False)