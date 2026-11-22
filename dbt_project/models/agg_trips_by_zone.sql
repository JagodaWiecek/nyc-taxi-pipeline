with zone_trips as (
    select
        'pickup' as zone_role,
        pickup_zone_id as zone_id,
        total_amount,
        trip_distance
    from {{ ref('fct_trips') }}

    union all

    select
        'dropoff' as zone_role,
        dropoff_zone_id as zone_id,
        total_amount,
        trip_distance
    from {{ ref('fct_trips') }}
)

select
    t.zone_role,
    t.zone_id,
    z.borough,
    z.zone,
    count(*) as trips,
    sum(t.total_amount) as total_revenue,
    sum(t.trip_distance) as total_distance_miles
from zone_trips t
left join {{ ref('dim_zone') }} z
    on t.zone_id = z.location_id
group by t.zone_role, t.zone_id, z.borough, z.zone