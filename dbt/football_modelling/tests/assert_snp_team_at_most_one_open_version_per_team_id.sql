select team_id
from {{ ref("snp_team") }}
where dbt_valid_to is null
group by team_id
having count(*) > 1
