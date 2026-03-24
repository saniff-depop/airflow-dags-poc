-- Month-by-month form guide: points per game by calendar month.
-- Reveals which teams peak early, fade late, or surge after January.
with monthly as (
    select
        t.team_id,
        t.team_name,
        d.month_name,
        d.month_number,
        count(*) as matches,
        sum(case
            when m.home_team_id = t.team_id and m.result_type = 'home_win' then 3
            when m.away_team_id = t.team_id and m.result_type = 'away_win' then 3
            when m.result_type = 'draw' then 1
            else 0
        end) as points
    from gold.fct_match m
    join gold.dim_date d on m.kickoff_date = d.date_day
    join gold.dim_team t
        on t.team_id in (m.home_team_id, m.away_team_id)
        and t.is_current
    group by t.team_id, t.team_name, d.month_name, d.month_number
)
select
    team_name,
    trim(month_name)                           as month,
    matches,
    points,
    round(points::numeric / matches, 2)        as ppg
from monthly
order by team_name, month_number;
