select
    pickup_hour,
    sum(trips) as trips,
    round(sum(total_distance_miles) / sum(trips), 2) as avg_distance_miles,
    round(sum(total_duration_seconds) / sum(trips) / 60, 1) as avg_duration_minutes,
    round(sum(total_fare) / sum(total_distance_miles), 2) as fare_per_mile,
    round(sum(total_distance_miles) / (sum(total_duration_seconds) / 3600), 1) as avg_speed_mph
from {{ ref('agg_efficiency_by_zone_hour') }}
group by pickup_hour