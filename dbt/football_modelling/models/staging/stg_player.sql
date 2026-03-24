with source as (
    select * from {{ source('silver', 'player') }}
)

select
    player_id,
    given_name,
    family_name,
    date_of_birth,
    nationality,
    preferred_foot
from source
