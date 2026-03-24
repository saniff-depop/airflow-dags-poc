with source as (
    select * from {{ source('silver', 'player_contract') }}
)

select
    player_contract_id,
    player_id,
    team_id,
    valid_from,
    valid_to,
    shirt_number,
    is_loan
from source
