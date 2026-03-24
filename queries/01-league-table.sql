-- Premier League table: final standings ranked by points, then goal difference.
select
    t.team_name,
    t.short_name,
    s.matches_played,
    s.wins,
    s.draws,
    s.losses,
    s.goals_for                         as gf,
    s.goals_against                     as ga,
    s.goals_for - s.goals_against       as gd,
    s.points
from gold.met_team_season_summary s
join gold.dim_team t
    on s.team_id = t.team_id
    and t.is_current
order by
    s.points desc,
    s.goals_for - s.goals_against desc,
    s.goals_for desc;
