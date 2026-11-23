select
    weekday(pickup_date) as weekday_number,
    date_format(pickup_date, 'EEEE') as weekday_name,
    sum(trips) as trips,
    round(sum(trips) / count(distinct pickup_date)) as avg_trips_per_day
from {{ ref('agg_demand_by_hour') }}
group by weekday(pickup_date), date_format(pickup_date, 'EEEE')
order by weekday_number