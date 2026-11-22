with expected_roles as (
    select 'pickup' as zone_role
    union all
    select 'dropoff' as zone_role
),

totals as (
    select zone_role, sum(trips) as total
    from {{ ref('agg_trips_by_zone') }}
    group by zone_role
),

fct as (
    select count(*) as total
    from {{ ref('fct_trips') }}
)

select
    e.zone_role,
    t.total,
    f.total as expected_total
from expected_roles e
left join totals t
    on e.zone_role = t.zone_role
cross join fct f
where t.total is null
   or t.total != f.total