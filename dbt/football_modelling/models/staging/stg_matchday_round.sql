with source as (
    select * from {{ source('silver', 'matchday_round') }}
)

select
    round_id,
    season_id,
    round_number,
    label
from source
