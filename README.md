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
- [x] Error handling (custom exceptions) and logging
- [x] Profiling of the bronze data
- [ ] Silver layer: cleaning and typing
- [ ] Gold layer with dbt
- [ ] GitHub Actions CI
- [ ] Scheduled Databricks job (no notebooks)
- [ ] Development guide (docs/development.md)

**Tests**
- [x] TLC client (mocked HTTP, no internet needed)
- [x] Bronze ingestor logic (mocked Spark)
- [ ] Silver layer (cleaning rules, deduplication)
- [ ] Gold layer (dbt tests: unique, not_null, relationships)
- [ ] End-to-end pipeline test on a small sample

## Project structure
```
├── main.py                  # entry point
├── pytest.ini
├── requirements.txt         # runtime dependencies
├── requirements-dev.txt     # test and lint tools
├── src/
│   ├── config.py            # paths, URLs, date range
│   ├── clients.py           # TlcClient: downloads files from TLC
│   ├── ingest.py            # BronzeIngestor: raw files -> bronze table
│   └── exceptions.py        # custom exceptions
├── analysis/
│   └── bronze_profile.py    # queries behind docs/data_quality.md
├── tests/
│   ├── test_clients.py
│   └── test_ingest.py
└── docs/
    ├── data_dictionary.md
    └── data_quality.md
    
```

## Data

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (yellow taxi, one Parquet file per month). Files are stored in the Unity Catalog Volume `workspace.bronze.raw`.
The date range is set in `src/config.py` (currently 2024-01 to 2024-12). Raw files are stored in the
Unity Catalog Volume `workspace.bronze.raw` and loaded into the table
`workspace.bronze.yellow_trips`.



## Getting started

1. Create the schema and Volume in Databricks:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE VOLUME IF NOT EXISTS workspace.bronze.raw;
   ```
2. In a Databricks notebook, download the files and load them into bronze:
   ```python
   from src.config import Config
   from src.clients import TlcClient
   from src.ingest import BronzeIngestor

   config = Config()
   TlcClient(config).download_range(config.start_month, config.end_month)
   BronzeIngestor(spark, config).ingest_range(config.start_month, config.end_month)
   ```
   Both steps are idempotent: existing files and loaded months are skipped.
3. Run the tests locally: `pip install -r requirements-dev.txt`, then `pytest`.

Already downloaded files are skipped, so the download can be safely re-run.
## Documentation

- [Data dictionary](docs/data_dictionary.md): columns and layer principles
- [Data quality](docs/data_quality.md): findings from profiling the bronze data
