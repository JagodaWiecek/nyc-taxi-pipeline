select
    payment_type,
    payment_type_name,
    sum(trips) as trips,
    round(sum(total_fare) / sum(trips), 2) as fare_per_trip,
    round(sum(total_revenue) / sum(trips), 2) as revenue_per_trip
from {{ ref('agg_revenue_and_tips') }}
group by payment_type, payment_type_name
order by payment_type