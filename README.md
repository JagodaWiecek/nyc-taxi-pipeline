# nyc-taxi-pipeline
A data engineering project that ingests NYC TLC yellow taxi trip data into
Databricks using Python and PySpark. The goal is a fully automated pipeline, medallion architecture, with tests, CI/CD and dbt models.
## Goal
Build an automated, tested data pipeline that turns raw NYC yellow taxi trip
data into analytics-ready tables, medallion architecture bronze, silver, gold,
so that we can answer where, when and how much people ride, and what drives
revenue and tips.

### Business questions

1. **Demand:** at what hours and on which days are there the most trips, and how does demand change over the year?
2. **Locations:** which pickup and dropoff zones, like airports or Manhattan, generate the most trips and revenue?
3. **Revenue and tips:** what drives revenue and tip size, payment type, time of day or distance?
4. **Efficiency:** what are the average distance, duration and price per mile, by hour and by zone?

## Status
**Code**
- [x] Config class with all project parameters
- [x] HTTP client that downloads monthly Parquet files into a Databricks Volume
- [x] Bronze layer: load raw files into a Delta table (PySpark)
- [ ] Silver layer: cleaning and typing
- [ ] Gold layer with dbt
- [ ] Data quality checks
- [ ] GitHub Actions CI
- [ ] Scheduled Databricks job (no notebooks)

**Tests**
- [x] Error Handling
- [x] TLC client (mocked HTTP, no internet needed)
- [x] Bronze layer (local Spark session)
- [ ] Silver layer (cleaning rules, deduplication)
- [ ] Gold layer (dbt tests: unique, not_null, relationships)
- [ ] End-to-end pipeline test on a small sample

## Project structure
```
├── main.py              # entry point
├── pytest.ini           # pytest settings
├── requirements.txt     # Python dependencies
├── src/
│   ├── config.py        # central configuration (paths, URLs, date range)
    ├── ingest.py        # BronzeIngestor: loads raw files into the bronze Delta table
    ├── exceptions.py    # inherit after exception class, to raise custom errors
│   └── clients.py       # TlcClient: downloads data from the TLC website
└── tests/
    ├── test_ingest.py   # unit tests for ingest
    └── test_clients.py  # unit tests for the client
```

## Data

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (yellow taxi, one Parquet file per month). Files are stored in the Unity Catalog Volume `workspace.bronze.raw`. The downloaded date range is set in `src/config.py`.


Files are stored in the Unity Catalog
Volume `workspace.bronze.raw`. The downloaded date range is set in `src/config.py`. Column descriptions follow the official TLC data dictionary. Money amounts are in US dollars, distances in miles. Timestamps are local New York time without a time zone.
### Source columns (bronze layer)
**Bronze** keeps the data as close to the source as possible, regardless of whether the values make sense. Invalid or illogical records (for example negative amounts, zero distance or an unusual passenger count) are intentionally **not** removed or corrected here. The only changes made in bronze are:
- column types are unified across monthly files (the source types vary between months),
- the airport fee column name is normalized to `Airport_fee`,
- a fixed set of source columns is selected,
- three technical columns are added (`_source_file`, `_year_month`, `_ingested_at`).
| Column | Type | Description |
|---|---|---|
| `VendorID` | int | Code of the technology provider that supplied the trip record. |
| `tpep_pickup_datetime` | timestamp_ntz | Date and time when the meter was engaged. |
| `tpep_dropoff_datetime` | timestamp_ntz | Date and time when the meter was disengaged. |
| `passenger_count` | long | Number of passengers, entered manually by the driver. |
| `trip_distance` | double | Trip distance in miles reported by the taximeter. |
| `RatecodeID` | long | Rate code in effect at the end of the trip: 1 = standard, 2 = JFK, 3 = Newark, 4 = Nassau or Westchester, 5 = negotiated fare, 6 = group ride, 99 = unknown. |
| `store_and_fwd_flag` | string | `Y` if the record was stored in the vehicle before being sent to the vendor (no connection), `N` otherwise. |
| `PULocationID` | int | TLC taxi zone where the meter was engaged (pickup). |
| `DOLocationID` | int | TLC taxi zone where the meter was disengaged (dropoff). |
| `payment_type` | long | How the passenger paid: 0 = flex fare, 1 = credit card, 2 = cash, 3 = no charge, 4 = dispute, 5 = unknown, 6 = voided trip. |
| `fare_amount` | double | Time-and-distance fare calculated by the meter. |
| `extra` | double | Miscellaneous extras and surcharges (e.g. rush hour and overnight charges). |
| `mta_tax` | double | MTA tax triggered automatically based on the metered rate. |
| `tip_amount` | double | Tip amount. Filled in automatically for credit card tips; **cash tips are not recorded**. |
| `tolls_amount` | double | Total tolls paid during the trip. |
| `improvement_surcharge` | double | Improvement surcharge assessed at the start of the trip. |
| `total_amount` | double | Total amount charged to the passenger. Does not include cash tips. |
| `congestion_surcharge` | double | Amount collected for the New York State congestion surcharge. |
| `Airport_fee` | double | Fee charged for pickups at LaGuardia and JFK airports. Spelled differently in some monthly files; normalized to `Airport_fee`. |

#### Technical columns added by the pipeline
| Column | Type | Description |
|---|---|---|
| `_source_file` | string | Path of the raw Parquet file the row came from. |
| `_year_month` | string | Month of the source file (`YYYY-MM`). Used to replace a single month without duplicating data. |
| `_ingested_at` | timestamp | When the row was loaded into the bronze table. |

#### Known data caveats
- Cash tips are missing, so tip analysis only makes sense for card payments
## Getting started

1. Create the schema and Volume in Databricks:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE VOLUME IF NOT EXISTS workspace.bronze.raw;
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the tests:
   ```
   pytest
   ```
4. Download the data (in a Databricks notebook or via `main.py`):
   ```python
   from src.config import Config
   from src.clients import TlcClient
   from src.ingest import BronzeIngestor

   config = Config()

   # Step 1: download monthly Parquet files into the raw Volume
   client = TlcClient(config)
   client.download_range(config.start_month, config.end_month)

   # Step 2: load the files into the bronze Delta table
   ingestor = BronzeIngestor(spark, config)
   ingestor.ingest_range(config.start_month, config.end_month)
   ```

Already downloaded files are skipped, so the download can be safely re-run.