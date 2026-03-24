select
    venue_id
from {{ ref("dim_venue") }}
where is_current
group by venue_id
having count(*) > 1
