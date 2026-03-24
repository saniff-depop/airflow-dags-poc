with source as (
    select * from {{ source('silver', 'team') }}
)

select
    team_id,
    name,
    short_name,
    founded_year,
    venue_id
from source
