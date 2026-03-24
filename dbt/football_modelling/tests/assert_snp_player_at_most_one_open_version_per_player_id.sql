select player_id
from {{ ref("snp_player") }}
where dbt_valid_to is null
group by player_id
having count(*) > 1
