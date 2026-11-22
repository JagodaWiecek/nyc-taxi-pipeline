select
    pickup_date,
    pickup_hour,
    count(*) as trips,
    sum(duration_seconds) as total_duration_seconds,
    round(avg(duration_seconds), 0) as avg_duration_seconds,
    round(percentile(duration_seconds, 0.5), 0) as median_duration_seconds,
    round(percentile(duration_seconds, 0.9), 0) as p90_duration_seconds,
    min(duration_seconds) as min_duration_seconds,
    max(duration_seconds) as max_duration_seconds
from {{ ref('fct_trips') }}
group by pickup_date, pickup_hour