select 1
from (select sum(trips) as total from {{ ref('agg_revenue_and_tips') }}) agg
cross join (select count(*) as total from {{ ref('fct_trips') }}) fct
where agg.total != fct.total