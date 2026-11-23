select
    zone_id,
    zone,
    borough,
    sum(case when zone_role = 'pickup' then trips end) as pickups,
    sum(case when zone_role = 'dropoff' then trips end) as dropoffs
from {{ ref('agg_trips_by_zone') }}
group by zone_id, zone, borough
order by pickups desc