select
    zone_id,
    zone,
    borough,
    trips
from {{ ref('agg_trips_by_zone') }}
where zone_role = 'dropoff'
order by trips desc
limit 10