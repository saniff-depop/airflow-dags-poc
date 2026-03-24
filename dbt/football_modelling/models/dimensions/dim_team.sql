select
    dbt_scd_id as team_scd_id,
    team_id,
    team_name,
    short_name,
    founded_year,
    venue_id,
    venue_name,
    venue_city,
    venue_capacity,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref("snp_team") }}
