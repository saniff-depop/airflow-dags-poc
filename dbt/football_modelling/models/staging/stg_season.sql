with source as (
    select * from {{ source('silver', 'season') }}
)

select
    season_id,
    competition_id,
    label,
    start_date,
    end_date
from source
