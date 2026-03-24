select
    player_id
from {{ ref("dim_player") }}
where is_current
group by player_id
having count(*) > 1
