## Layer principles
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