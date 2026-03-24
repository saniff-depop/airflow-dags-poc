select
    dbt_scd_id as player_scd_id,
    player_id,
    given_name,
    family_name,
    date_of_birth,
    nationality,
    preferred_foot,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref("snp_player") }}
