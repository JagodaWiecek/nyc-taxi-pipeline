# Data quality of the bronze table

All numbers come from `analysis/bronze_profile.py`, run on the 12 monthly
files of 2024 (41,169,720 rows). Each table below is named after the
function that produced it. Bronze keeps the data unchanged, so these
numbers describe the raw source, not problems fixed by the pipeline.

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
type (`payment_type = 0`), not random entry errors. The nulls are kept in
silver.

## Passenger counts (`passenger_count_distribution`)

Values 1-6 cover almost all trips. 401,354 trips report 0 passengers and
284 trips report 7-9.

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

To be filled in once the rules are agreed.