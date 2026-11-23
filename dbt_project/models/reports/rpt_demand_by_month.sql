select
    date_format(pickup_date, 'yyyy-MM') as pickup_month,
    sum(trips) as trips,
    round(sum(trips) / count(distinct pickup_date)) as avg_trips_per_day
from {{ ref('agg_demand_by_hour') }}
group by date_format(pickup_date, 'yyyy-MM')
order by pickup_month