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
- [ ] Gold layer with dbt (fct_trips done, dimensions and analysis pending)
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
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml         # connection settings read from environment variables
│   └── models/
│       ├── sources.yml      # silver table as a dbt source
│       ├── schema.yml       # descriptions and tests
│       └── fct_trips.sql    # fact table: one row per trip
└── docs/
    ├── data_dictionary.md
    └── data_quality.md
    
```

## Data

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (yellow taxi, one Parquet file per month). Files are stored in the Unity Catalog Volume `workspace.bronze.raw`.
The date range is set in `src/config.py` (currently 2024-01 to 2024-12).
| Table | Layer | Content |
|---|---|---|
| `workspace.bronze.yellow_trips` | bronze | trips as received |
| `workspace.bronze.taxi_zones` | bronze | taxi zone lookup as received |
| `workspace.silver.yellow_trips` | silver | cleaned trips with quality flags |
| `workspace.silver.taxi_zones` | silver | cleaned zone lookup |
| `workspace.gold.fct_trips` | gold | one row per trip (dbt) |

Both bronze and silver are processed **one month at a time**, using the
`_year_month` column. A new month is processed without recomputing the
previous ones, and re-running the pipeline skips months that are already
loaded. The zone lookup is a single small file, loaded once.

## Getting started

1. Create the schemas and the Volume in Databricks (SQL editor):
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE SCHEMA IF NOT EXISTS workspace.silver;
   CREATE SCHEMA IF NOT EXISTS workspace.gold;
   CREATE VOLUME IF NOT EXISTS workspace.bronze.raw;
   ```
2. In a Databricks notebook, download the files and build bronze and silver:
   ```python
   from src.config import Config
   from src.clients import TlcClient
   from src.ingest import BronzeIngestor
   from src.transform import SilverTransformer

   config = Config()
   TlcClient(config).download_range(config.start_month, config.end_month)
   BronzeIngestor(spark, config).ingest_range(config.start_month, config.end_month)
   SilverTransformer(spark, config).transform_range(config.start_month, config.end_month)

   TlcClient(config).download_zone_lookup()
   BronzeIngestor(spark, config).ingest_zone_lookup()
   SilverTransformer(spark, config).transform_zone_lookup()
   ```
   All steps are idempotent: existing files and months already loaded are
   skipped. To reprocess months (for example after changing a cleaning rule),
   pass `force=True` to `ingest_range` or `transform_range`.

   The zone lookup is skipped once loaded. To refresh it, delete taxi_zone_lookup.csv from the Volume and call the two zone methods again (with force=True for the table).
3. Set up the local environment
   ```
   python -m venv .venv
   .venv\Scripts\activate.bat        # Windows cmd
   pip install -r requirements-dev.txt
   ```
4. Run the tests:
   ```
   pytest
   ```
5. Build the gold layer with dbt. It needs a running SQL warehouse and a
   Databricks personal access token. Set the connection details as
   environment variables (Windows cmd syntax, no quotes; the values stay out of the repository):
   ```
   set DATABRICKS_HOST=<server hostname, without https://>
   set DATABRICKS_HTTP_PATH=<HTTP path of the SQL warehouse>
   set DATABRICKS_TOKEN=<personal access token>
   ```
   Then, from the `dbt_project` folder:
   ```
   dbt debug --profiles-dir .
   dbt run --profiles-dir .
   dbt test --profiles-dir .
   ```

Already downloaded files are skipped, so the download can be safely re-run.
## Documentation

- [Data dictionary](docs/data_dictionary.md): columns and layer principles
- [Data quality](docs/data_quality.md): findings from profiling the bronze data
