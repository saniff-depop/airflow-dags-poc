-- Weekend vs midweek matches: do midweek fixtures produce more or fewer goals?
select
    case when d.is_weekend then 'Weekend' else 'Midweek' end as match_slot,
    count(*)                    as matches,
    sum(m.total_goals)          as total_goals,
    round(avg(m.total_goals), 2) as avg_goals_per_match,
    sum(case when m.result_type = 'draw' then 1 else 0 end) as draws,
    round(100.0 * sum(case when m.result_type = 'draw' then 1 else 0 end) / count(*), 1) as draw_pct
from gold.fct_match m
join gold.dim_date d on m.kickoff_date = d.date_day
group by d.is_weekend
order by match_slot;
