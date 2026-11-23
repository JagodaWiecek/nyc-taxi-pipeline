select
    zone_id,
    zone,
    borough,
    trips,
    round(total_revenue, 0) as total_revenue,
    round(total_revenue / trips, 2) as revenue_per_trip
from {{ ref('agg_trips_by_zone') }}
where zone_role = 'pickup'
order by trips desc