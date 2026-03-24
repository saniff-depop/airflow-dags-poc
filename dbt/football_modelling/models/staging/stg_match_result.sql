with source as (
    select * from {{ source('silver', 'match_result') }}
)

select
    fixture_id,
    home_goals,
    away_goals,
    home_xg,
    away_xg
from source
