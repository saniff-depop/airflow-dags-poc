with source as (
    select * from {{ source('silver', 'fixture') }}
)

select
    fixture_id,
    season_id,
    round_id,
    home_team_id,
    away_team_id,
    venue_id,
    kickoff_utc,
    status
from source
