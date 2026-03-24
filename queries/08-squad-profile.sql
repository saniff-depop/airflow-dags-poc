-- Squad profile: squad size, average contract length, and loan dependency
-- per team based on active contracts.
select
    t.team_name,
    count(*)                                                as squad_size,
    sum(case when c.is_loan then 1 else 0 end)              as loan_players,
    round(100.0 * sum(case when c.is_loan then 1 else 0 end)
        / count(*), 1)                                      as loan_pct,
    round(avg(c.contract_duration_days))                    as avg_contract_days,
    round(avg(c.contract_duration_days) / 365.25, 1)        as avg_contract_years,
    min(c.valid_from)                                       as earliest_contract,
    max(c.valid_to)                                         as latest_expiry
from gold.fct_player_contract c
join gold.dim_team t
    on c.team_id = t.team_id
    and t.is_current
where c.valid_to is null
   or c.valid_to >= current_date
group by t.team_name
order by squad_size desc;
