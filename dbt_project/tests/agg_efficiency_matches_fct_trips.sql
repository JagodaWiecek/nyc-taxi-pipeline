select 1
from (select sum(trips) as total from {{ ref('agg_efficiency_by_zone_hour') }}) agg
cross join (
    select count(*) as total
    from {{ ref('fct_trips') }}
    where not has_zero_distance
      and not has_negative_amount
      and not has_outlier_distance
) fct
where agg.total != fct.total