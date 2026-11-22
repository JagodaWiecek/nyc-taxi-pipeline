select 1
from (select sum(trips) as total from {{ ref('agg_demand_by_hour') }}) agg
cross join (select count(*) as total from {{ ref('fct_trips') }}) fct
where agg.total != fct.total