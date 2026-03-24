select dbt_scd_id
from {{ ref("snp_venue") }}
where dbt_valid_to is not null
    and dbt_valid_from >= dbt_valid_to
