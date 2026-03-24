-- Cumulative points by matchday: shows the league table evolving over time.
-- Useful for plotting title races and relegation battles.
with match_points as (
    select
        m.home_team_id as team_id,
        r.round_number,
        case m.result_type
            when 'home_win' then 3
            when 'draw'     then 1
            else 0
        end as points
    from gold.fct_match m
    join gold.dim_matchday_round r on m.round_id = r.round_id

    union all

    select
        m.away_team_id as team_id,
        r.round_number,
        case m.result_type
            when 'away_win' then 3
            when 'draw'     then 1
            else 0
        end as points
    from gold.fct_match m
    join gold.dim_matchday_round r on m.round_id = r.round_id
)
select
    t.team_name,
    mp.round_number,
    sum(mp.points) over (
        partition by mp.team_id
        order by mp.round_number
    ) as cumulative_points
from match_points mp
join gold.dim_team t
    on mp.team_id = t.team_id
    and t.is_current
order by mp.round_number, cumulative_points desc;
