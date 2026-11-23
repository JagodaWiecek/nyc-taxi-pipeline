select
    pickup_hour,
    sum(trips) as trips,
    round(sum(total_duration_seconds) / sum(trips) / 60, 1) as avg_duration_minutes
from {{ ref('agg_demand_by_hour') }}
group by pickup_hour
order by pickup_hour