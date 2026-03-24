select venue_id
from {{ ref("snp_venue") }}
where dbt_valid_to is null
group by venue_id
having count(*) > 1
