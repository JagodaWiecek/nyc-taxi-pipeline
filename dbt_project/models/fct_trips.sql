{% set key_columns = [
    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
    'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
    'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Airport_fee'
] %}

select
    md5(concat_ws('|',
        {% for column in key_columns -%}
        coalesce(cast({{ column }} as string), ''){% if not loop.last %},{% endif %}
        {% endfor %}
    )) as trip_id,
    VendorID as vendor_id,
    tpep_pickup_datetime as pickup_at,
    tpep_dropoff_datetime as dropoff_at,
    cast(tpep_pickup_datetime as date) as pickup_date,
    hour(tpep_pickup_datetime) as pickup_hour,
    PULocationID as pickup_zone_id,
    DOLocationID as dropoff_zone_id,
    payment_type,
    passenger_count,
    trip_distance,
    timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) as duration_seconds,
    fare_amount,
    tip_amount,
    total_amount,
    has_negative_amount,
    has_outlier_distance,
    has_zero_distance
from {{ source('silver', 'yellow_trips') }}