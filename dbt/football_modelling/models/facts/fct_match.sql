{{
    config(
        materialized='incremental',
        unique_key='fixture_id',
        on_schema_change='append_new_columns',
    )
}}

with fixtures as (

    select *
    from {{ ref('stg_fixture') }}
    where status = 'finished'

    {% if is_incremental() %}
        and (
            (select max(kickoff_utc) from {{ this }}) is null
            or kickoff_utc > (select max(kickoff_utc) from {{ this }})
        )
    {% endif %}

),

results as (

    select *
    from {{ ref('stg_match_result') }}

),

match_rows as (

    select
        f.fixture_id,
        f.season_id,
        f.round_id,
        f.home_team_id,
        f.away_team_id,
        f.venue_id,
        f.kickoff_utc,
        cast(f.kickoff_utc as date) as kickoff_date,
        f.status,
        r.home_goals,
        r.away_goals,
        r.home_xg,
        r.away_xg,
        r.home_goals - r.away_goals as goal_difference,
        r.home_goals + r.away_goals as total_goals,
        case
            when r.home_goals > r.away_goals then 'home_win'
            when r.home_goals < r.away_goals then 'away_win'
            else 'draw'
        end as result_type
    from fixtures as f
    inner join results as r on f.fixture_id = r.fixture_id

)

select
    m.fixture_id,
    m.season_id,
    m.round_id,
    m.home_team_id,
    m.away_team_id,
    ht.team_scd_id as home_team_scd_id,
    aw.team_scd_id as away_team_scd_id,
    m.venue_id,
    mv.venue_scd_id as match_venue_scd_id,
    m.kickoff_utc,
    m.kickoff_date,
    m.status,
    m.home_goals,
    m.away_goals,
    m.home_xg,
    m.away_xg,
    m.goal_difference,
    m.total_goals,
    m.result_type
from match_rows as m
inner join {{ ref('dim_team') }} as ht
    on m.home_team_id = ht.team_id
    and m.kickoff_utc >= ht.dbt_valid_from
    and (ht.dbt_valid_to is null or m.kickoff_utc < ht.dbt_valid_to)
inner join {{ ref('dim_team') }} as aw
    on m.away_team_id = aw.team_id
    and m.kickoff_utc >= aw.dbt_valid_from
    and (aw.dbt_valid_to is null or m.kickoff_utc < aw.dbt_valid_to)
inner join {{ ref('dim_venue') }} as mv
    on m.venue_id = mv.venue_id
    and m.kickoff_utc >= mv.dbt_valid_from
    and (mv.dbt_valid_to is null or m.kickoff_utc < mv.dbt_valid_to)
