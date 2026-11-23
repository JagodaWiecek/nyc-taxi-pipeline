# Data quality
All numbers come from `analysis/bronze_profile.py`, run on the 12 monthly
files of 2024 (41,169,720 rows). Each table is named after the function
that produced it. The first sections describe the raw source, bronze keeps
the data unchanged. The later sections describe the silver rules and their results.

## Suspicious records (`suspicious_records`)

One trip can fall into several categories, so the counts must not be added up.

| Check | Rows | Share |
|---|---|---|
| `passenger_count` is null | 4,091,232 | 9.94% |
| `trip_distance` = 0 | 776,305 | 1.89% |
| `fare_amount` < 0 | 731,024 | 1.78% |
| `total_amount` < 0 | 609,344 | 1.48% |
| dropoff = pickup time | 11,935 | 0.03% |
| dropoff before pickup | 1,575 | 0.004% |
| pickup date outside the file's month | 420 | 0.001% |

## Missing passenger counts

Table `nulls_by_payment_type`: every row with `payment_type = 0`
(4,091,232 rows) has a null `passenger_count`, and no row of any other
payment type does.

Table `nulls_by_vendor`: for vendors 1, 2 and 6, the number of nulls in
`passenger_count`, `RatecodeID`, `congestion_surcharge` and `Airport_fee`
is identical.

**Interpretation (hypothesis):** the nulls are a property of the record
type (`payment_type = 0`, Flex Fare), not random entry errors. This is
consistent with Flex Fare trips not reporting these fields; the cause is
not confirmed. The nulls are kept in silver.

The `payment_type` codes match the TLC Yellow Taxi Data Dictionary
(18 Mar 2025).

## Passenger counts (`passenger_count_distribution`)

Values 1-6 cover almost all trips. 401,354 trips report 0 passengers and
284 trips report 7-9. TLC describes the field as driver-reported and does
not guarantee its accuracy.

## Negative fares (`negative_fare_by_payment_type`)

| `payment_type` | Rows |
|---|---|
| 4 | 368,654 |
| 2 | 144,430 |
| 0 | 127,092 |
| 3 | 90,609 |
| 1 | 239 |

Types 3 and 4 account for 63% of the negative fares. **Hypothesis:** many
negative amounts are adjustments or disputed trips rather than errors.
This is not verified.

## Zero distance (`zero_distance_context`)

Of 776,305 trips with zero distance, 714,784 (92%) have a positive fare
and 373,124 (48%) start and end in the same zone. The remaining 52% start
and end in different zones, which contradicts a distance of zero.

## Decisions for the silver layer

| Case | Rows (2024) | Decision |
|---|---|---|
| Dropoff before or at pickup time | 1,575 + 11,935 | **dropped** (impossible duration) |
| Pickup date outside the file's month | 420 | **dropped** (outside the expected range) |
| Negative `fare_amount` or `total_amount` | up to 733,825 | **kept**, flagged `has_negative_amount` |
| `trip_distance` = 0 | 776,305 | **kept**, flagged `has_zero_distance` |
| `passenger_count` outside 1-6 (including 0) | 401,638 | set to **null** |
| `passenger_count` already null | 4,091,232 | kept as is |
| Rows identical in all 19 source columns | 4 surplus | **deduplicated** (one copy kept) |
| Processing unit | one month | **per-month processing**, like bronze |

Rows are flagged instead of dropped when they may be real events
(for example disputed trips). Gold models decide per question whether to
exclude them.

## Duplicates

With a weak key (vendor, pickup and dropoff time, zones, total amount),
7 rows repeat. Only 4 groups are identical in all 19 source columns, and
each lies within a single monthly file. The source has no trip identifier,
so identical rows cannot be proven to be duplicates (two real trips can
share every value, for example very short trips in one zone).

Silver nevertheless **removes rows identical in all 19 source columns**
(keeping one copy), as a safeguard for larger date ranges. In the 2024
data this affects 4 surplus rows. Deduplication works within one month
(see Processing model).

## Processing model

Silver is built **month by month**, the same unit as the monthly source
files and the bronze layer. Each month is read from bronze, cleaned and
written to the silver table, replacing only the rows with that
`_year_month` (Delta `replaceWhere`).

Why not clean the whole table at once:

- New data arrives monthly, so only the new month needs processing.
- A failure in one month does not block the others. All months are
  attempted and a single error listing the failed months is raised at the end.
- Re-running is cheap: months already in silver are skipped.

After changing a cleaning rule, reprocess existing months with
`transform_range(start, end, force=True)`.

## Results of the silver layer

Run on the 2024 data. Rows removed by the rules on impossible durations,
pickup dates outside the file's month and exact duplicates.

| Month | Bronze rows | Silver rows | Removed |
|---|---|---|---|
| 2024-01 | 2,964,624 | 2,963,736 | 888 |
| 2024-02 | 3,007,526 | 3,006,707 | 819 |
| 2024-03 | 3,582,628 | 3,581,477 | 1,151 |
| 2024-04 | 3,514,289 | 3,513,191 | 1,098 |
| 2024-05 | 3,723,833 | 3,722,653 | 1,180 |
| 2024-06 | 3,539,193 | 3,538,002 | 1,191 |
| 2024-07 | 3,076,903 | 3,075,787 | 1,116 |
| 2024-08 | 2,979,183 | 2,978,114 | 1,069 |
| 2024-09 | 3,633,030 | 3,631,820 | 1,210 |
| 2024-10 | 3,833,771 | 3,832,727 | 1,044 |
| 2024-11 | 3,646,369 | 3,644,458 | 1,911 |
| 2024-12 | 3,668,371 | 3,667,114 | 1,257 |
| **Total** | **41,169,720** | **41,155,786** | **13,934** |

The total matches the profiling of bronze: 1,575 + 11,935 + 420 = 13,930
rows with impossible times, plus 4 surplus duplicates. Rows with negative
amounts, zero distance or an unusual passenger count are not removed; they
are flagged or set to null.

### Flags and nulls in the silver table

| Column | Rows | Share of 41,155,786 |
|---|---|---|
| `has_negative_amount` = true | 733,825 | 1.78% |
| `has_zero_distance` = true | 765,175 | 1.86% |
| `passenger_count` is null | 4,488,450 | 10.91% |

The null count is lower than the 4,492,870 expected from bronze
(4,091,232 missing + 401,638 outside 1-6) because some of the 13,934
removed rows had a null or zero passenger count. No row has a dropoff at
or before its pickup time.

## Taxi zone lookup

Source: TLC `taxi_zone_lookup.csv` (265 rows, `LocationID` 1-265).

- **Zone names are not unique.** `Corona` has IDs 56 and 57; `Governor's Island/Ellis Island/Liberty Island` has IDs 103, 104 and 105. Always join and group on the ID, never on the name.
- **IDs 264 and 265 are placeholders** (`Unknown` and `Outside of NYC`). In the source, missing values are the text `N/A`; bronze keeps them as is, silver turns them into NULL, and the gold `dim_zone` shows them as `Unknown`.
- **Silver rule:** `location_id` must be not null and unique, otherwise the load fails with `SilverError` and nothing is written.
- **Not refreshed automatically:** the file is downloaded once and skipped if it already exists. To pick up a new TLC version, delete the file from the Volume and reload with `force=True`.
- **Verified in gold:** every pickup and dropoff zone ID in `fct_trips` exists in `dim_zone` (dbt `relationships` tests, 2024 data).

## Gold layer checks

dbt tests (`dbt build`) run on the gold tables. On the 2024 data all of them pass:

- `fct_trips.trip_id` is unique and not null: one row per distinct trip, 41,155,786 rows, the same as silver.
- Every pickup and dropoff zone ID in `fct_trips` exists in `dim_zone`, and no trip has an empty zone ID.
- Every `payment_type` in `fct_trips` has a match in `dim_payment_type` (codes 0-6 from the TLC data dictionary).
- `duration_seconds`, `has_negative_amount` and `has_zero_distance` are never null.
- Aggregates match the fact table: the sum of trips in `agg_demand_by_hour` equals the rows of `fct_trips`, and in `agg_trips_by_zone` each role (pickup, dropoff) contains all trips (custom dbt tests in `dbt_project/tests/`).
- No trip has a negative or missing `trip_distance` or a missing `payment_type` (`not_null` tests on `agg_revenue_and_tips`).
- `agg_efficiency_by_zone_hour` excludes trips flagged `has_zero_distance` or `has_negative_amount`: 1,442,800 trips (3.5%), leaving 39,712,986. 56,200 trips carry both flags.