-- xG over/under-performers: teams whose actual goals deviate most from their
-- expected goals. Positive delta = clinical finishing; negative = wasteful.
select
    t.team_name,
    s.goals_for,
    round(s.xg_for::numeric, 2)                            as xg_for,
    round((s.goals_for - s.xg_for)::numeric, 2)            as attacking_delta,
    s.goals_against,
    round(s.xg_against::numeric, 2)                        as xg_against,
    round((s.xg_against - s.goals_against)::numeric, 2)    as defensive_delta
from gold.met_team_season_summary s
join gold.dim_team t
    on s.team_id = t.team_id
    and t.is_current
order by attacking_delta desc;
