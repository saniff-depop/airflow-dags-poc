-- Highest-scoring matchdays: aggregate goals per round to find the most
-- entertaining weekends of football.
select
    r.round_label,
    count(*)              as matches,
    sum(m.total_goals)    as total_goals,
    round(avg(m.total_goals), 2) as avg_goals_per_match,
    max(m.total_goals)    as most_goals_in_a_match
from gold.fct_match m
join gold.dim_matchday_round r on m.round_id = r.round_id
group by r.round_label, r.round_number
order by r.round_number;
