select
    zone_id,
    zone,
    borough,
    sum(trips) as trips,
    round(sum(total_distance_miles) * {{ var('km_per_mile') }} / sum(trips), 2) as avg_distance_km,
    round(sum(total_duration_seconds) / sum(trips) / 60, 1) as avg_duration_minutes,
    round(sum(total_fare) / (sum(total_distance_miles) * {{ var('km_per_mile') }}), 2) as fare_per_km,
    round(sum(total_distance_miles) * {{ var('km_per_mile') }} / (sum(total_duration_seconds) / 3600), 1) as avg_speed_kmh
from {{ ref('agg_efficiency_by_zone_hour') }}
group by zone_id, zone, borough