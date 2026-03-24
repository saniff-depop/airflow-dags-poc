-- Venue fortress rating: which grounds are the hardest to visit?
-- Ranks stadiums by home win percentage and average home goal difference.
select
    v.venue_name,
    t.team_name,
    v.capacity,
    count(*)                                                    as home_matches,
    sum(case when m.result_type = 'home_win' then 1 else 0 end) as home_wins,
    round(100.0 * sum(case when m.result_type = 'home_win' then 1 else 0 end)
        / count(*), 1)                                          as home_win_pct,
    round(avg(m.goal_difference), 2)                            as avg_goal_diff,
    round(avg(m.home_goals), 2)                                 as avg_home_goals
from gold.fct_match m
join gold.dim_venue v
    on m.venue_id = v.venue_id
    and m.kickoff_utc >= v.dbt_valid_from
    and (v.dbt_valid_to is null or m.kickoff_utc < v.dbt_valid_to)
join gold.dim_team t
    on m.home_team_id = t.team_id
    and m.kickoff_utc >= t.dbt_valid_from
    and (t.dbt_valid_to is null or m.kickoff_utc < t.dbt_valid_to)
group by v.venue_name, t.team_name, v.capacity
order by home_win_pct desc, avg_goal_diff desc;
