# nyc-taxi-pipeline
A data engineering project that ingests NYC TLC yellow taxi trip data into
Databricks using Python and PySpark. The goal is a fully automated pipeline, medallion architecture, with tests, CI/CD and dbt models.

## Status
**Code**
- [x] Config class with all project parameters
- [x] HTTP client that downloads monthly Parquet files into a Databricks Volume
- [ ] Bronze layer: load raw files into a Delta table (PySpark)
- [ ] Silver layer: cleaning and typing
- [ ] Gold layer with dbt
- [ ] Data quality checks
- [ ] GitHub Actions CI
- [ ] Scheduled Databricks job (no notebooks)

**Tests**
- [x] TLC client (mocked HTTP, no internet needed)
- [ ] Bronze layer (local Spark session)
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
│   └── clients.py       # TlcClient: downloads data from the TLC website
└── tests/
    └── test_clients.py  # unit tests for the client
```

## Data

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(yellow taxi, one Parquet file per month). Files are stored in the Unity Catalog
Volume `workspace.bronze.raw`. The downloaded date range is set in `src/config.py`.

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

   config = Config()
   client = TlcClient(config)
   client.download_range(config.start_month, config.end_month)
   ```

Already downloaded files are skipped, so the download can be safely re-run.