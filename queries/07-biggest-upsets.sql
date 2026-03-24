-- Biggest upsets by xG: matches where the winning team had significantly
-- lower expected goals than the loser.
select
    r.round_label,
    ht.team_name    as home_team,
    at_.team_name   as away_team,
    m.home_goals,
    m.away_goals,
    m.result_type,
    round(m.home_xg::numeric, 2) as home_xg,
    round(m.away_xg::numeric, 2) as away_xg,
    round(case
        when m.result_type = 'home_win' then m.away_xg - m.home_xg
        when m.result_type = 'away_win' then m.home_xg - m.away_xg
    end::numeric, 2) as xg_upset_margin
from gold.fct_match m
join gold.dim_team ht
    on m.home_team_id = ht.team_id
    and m.kickoff_utc >= ht.dbt_valid_from
    and (ht.dbt_valid_to is null or m.kickoff_utc < ht.dbt_valid_to)
join gold.dim_team at_
    on m.away_team_id = at_.team_id
    and m.kickoff_utc >= at_.dbt_valid_from
    and (at_.dbt_valid_to is null or m.kickoff_utc < at_.dbt_valid_to)
join gold.dim_matchday_round r on m.round_id = r.round_id
where m.result_type != 'draw'
order by xg_upset_margin desc nulls last
limit 15;
