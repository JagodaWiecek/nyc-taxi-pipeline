with trips as (
    select
        payment_type,
        pickup_hour,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance,
        case
            when trip_distance < 0 then null
            when trip_distance = 0 then '0: zero'
            when trip_distance < 1 then '1: 0-1 mi'
            when trip_distance < 2 then '2: 1-2 mi'
            when trip_distance < 5 then '3: 2-5 mi'
            when trip_distance < 10 then '4: 5-10 mi'
            when trip_distance < 20 then '5: 10-20 mi'
            when trip_distance >= 20 then '6: 20+ mi'
        end as distance_bucket
    from {{ ref('fct_trips') }}
)

select
    t.payment_type,
    p.payment_type_name,
    t.pickup_hour,
    t.distance_bucket,
    count(*) as trips,
    sum(t.fare_amount) as total_fare,
    sum(t.tip_amount) as total_tip,
    sum(t.total_amount) as total_revenue,
    sum(t.trip_distance) as total_distance_miles
from trips t
left join {{ ref('dim_payment_type') }} p
    on t.payment_type = p.payment_type_id
group by t.payment_type, p.payment_type_name, t.pickup_hour, t.distance_bucket