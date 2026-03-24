with matches as (
    select * from {{ ref('fct_match') }}
),

home as (
    select
        home_team_id    as team_id,
        season_id,
        count(*)        as matches_played,
        sum(case when result_type = 'home_win' then 1 else 0 end) as wins,
        sum(case when result_type = 'draw' then 1 else 0 end)     as draws,
        sum(case when result_type = 'away_win' then 1 else 0 end) as losses,
        sum(home_goals) as goals_for,
        sum(away_goals) as goals_against,
        sum(home_xg)    as xg_for,
        sum(away_xg)    as xg_against
    from matches
    group by home_team_id, season_id
),

away as (
    select
        away_team_id    as team_id,
        season_id,
        count(*)        as matches_played,
        sum(case when result_type = 'away_win' then 1 else 0 end) as wins,
        sum(case when result_type = 'draw' then 1 else 0 end)     as draws,
        sum(case when result_type = 'home_win' then 1 else 0 end) as losses,
        sum(away_goals) as goals_for,
        sum(home_goals) as goals_against,
        sum(away_xg)    as xg_for,
        sum(home_xg)    as xg_against
    from matches
    group by away_team_id, season_id
)

select
    coalesce(h.team_id, a.team_id)      as team_id,
    coalesce(h.season_id, a.season_id)  as season_id,
    coalesce(h.matches_played, 0) + coalesce(a.matches_played, 0) as matches_played,
    coalesce(h.wins, 0) + coalesce(a.wins, 0)       as wins,
    coalesce(h.draws, 0) + coalesce(a.draws, 0)     as draws,
    coalesce(h.losses, 0) + coalesce(a.losses, 0)   as losses,
    coalesce(h.goals_for, 0) + coalesce(a.goals_for, 0)         as goals_for,
    coalesce(h.goals_against, 0) + coalesce(a.goals_against, 0) as goals_against,
    coalesce(h.xg_for, 0) + coalesce(a.xg_for, 0)              as xg_for,
    coalesce(h.xg_against, 0) + coalesce(a.xg_against, 0)      as xg_against,
    (coalesce(h.wins, 0) + coalesce(a.wins, 0)) * 3
        + (coalesce(h.draws, 0) + coalesce(a.draws, 0))         as points
from home h
full outer join away a on h.team_id = a.team_id and h.season_id = a.season_id
