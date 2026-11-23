select
    t.pickup_zone_id as zone_id,
    z.borough,
    z.zone,
    t.pickup_hour,
    count(*) as trips,
    sum(t.trip_distance) as total_distance_miles,
    sum(t.duration_seconds) as total_duration_seconds,
    sum(t.fare_amount) as total_fare
from {{ ref('fct_trips') }} t
left join {{ ref('dim_zone') }} z
    on t.pickup_zone_id = z.location_id
where not t.has_zero_distance
  and not t.has_negative_amount
  and not t.has_outlier_distance
group by t.pickup_zone_id, z.borough, z.zone, t.pickup_hour