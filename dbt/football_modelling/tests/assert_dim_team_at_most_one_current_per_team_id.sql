select
    team_id
from {{ ref("dim_team") }}
where is_current
group by team_id
having count(*) > 1
