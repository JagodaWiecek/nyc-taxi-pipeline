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
- [x] Silver layer: cleaning, typing and duplicates
- [ ] Gold layer with dbt
- [ ] GitHub Actions CI
- [ ] Scheduled Databricks job (no notebooks)
- [ ] Development guide (docs/development.md)
- [ ] Apply catalog comments as part of the pipeline run (main.py)

**Tests**
- [x] TLC client (mocked HTTP, no internet needed)
- [x] Bronze ingestor logic (mocked Spark)
- [x] Silver transformer logic (mocked Spark)
- [ ] Silver cleaning rules on a real Spark session (written, waiting for CI)
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
│   ├── transform.py         # SilverTransformer: bronze -> silver (cleaning rules)
│   ├── catalog_docs.py      # table and column comments in Unity Catalog
│   └── exceptions.py        # custom exceptions
├── analysis/
│   └── bronze_profile.py    # queries behind docs/data_quality.md
├── tests/
│   ├── test_clients.py
│   ├── test_ingest.py
│   └── test_transform.py
└── docs/
    ├── data_dictionary.md
    └── data_quality.md
    
```

## Data

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (yellow taxi, one Parquet file per month). Files are stored in the Unity Catalog Volume `workspace.bronze.raw`.
The date range is set in `src/config.py` (currently 2024-01 to 2024-12). Raw files are stored in the
Unity Catalog Volume `workspace.bronze.raw` and loaded into the table
`workspace.bronze.yellow_trips`.

Both bronze and silver are processed **one month at a time**, using the
`_year_month` column. A new month is processed without recomputing the
previous ones, and re-running the pipeline skips months that are already
loaded.

## Getting started

1. Create the schemas and the Volume in Databricks (SQL editor):
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE SCHEMA IF NOT EXISTS workspace.silver;
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
3. Clean the bronze data into the silver table:
   ```python
   from src.transform import SilverTransformer

   SilverTransformer(spark, config).transform_range(config.start_month, config.end_month)
   ```
   All steps are idempotent: existing files and months already loaded are
   skipped. To reprocess months (for example after changing a cleaning rule),
   pass `force=True` to `ingest_range` or `transform_range`.
4. Run the tests locally: `pip install -r requirements-dev.txt`, then `pytest`.

Already downloaded files are skipped, so the download can be safely re-run.
## Documentation

- [Data dictionary](docs/data_dictionary.md): columns and layer principles
- [Data quality](docs/data_quality.md): findings from profiling the bronze data
