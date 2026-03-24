select
    dbt_scd_id as venue_scd_id,
    venue_id,
    venue_name,
    city,
    capacity,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref("snp_venue") }}
