select
    distance_bucket,
    sum(trips) as trips,
    round(sum(total_tip) / sum(total_fare) * 100, 1) as tip_pct_of_fare
from {{ ref('agg_revenue_and_tips') }}
where payment_type = 1
group by distance_bucket
order by distance_bucket