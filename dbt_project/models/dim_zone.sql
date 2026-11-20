select
    location_id,
    coalesce(borough, 'Unknown') as borough,
    coalesce(zone, 'Unknown') as zone,
    coalesce(service_zone, 'Unknown') as service_zone
from {{ source('silver', 'taxi_zones') }}