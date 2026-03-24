-- Home vs away performance split: identifies teams that are significantly
-- stronger at home or on the road.
with split as (
    select
        t.team_name,
        sum(case when m.home_team_id = t.team_id and m.result_type = 'home_win' then 3
                 when m.home_team_id = t.team_id and m.result_type = 'draw'     then 1
                 else 0 end)  as home_points,
        sum(case when m.away_team_id = t.team_id and m.result_type = 'away_win' then 3
                 when m.away_team_id = t.team_id and m.result_type = 'draw'     then 1
                 else 0 end)  as away_points,
        count(case when m.home_team_id = t.team_id then 1 end) as home_matches,
        count(case when m.away_team_id = t.team_id then 1 end) as away_matches
    from gold.fct_match m
    join gold.dim_team t
        on t.team_id in (m.home_team_id, m.away_team_id)
        and t.is_current
    group by t.team_name
)
select
    team_name,
    home_matches,
    home_points,
    round(home_points::numeric / nullif(home_matches, 0), 2)  as home_ppg,
    away_matches,
    away_points,
    round(away_points::numeric / nullif(away_matches, 0), 2)  as away_ppg,
    home_points - away_points                                  as home_advantage
from split
order by home_advantage desc;
