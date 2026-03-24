with contracts as (
    select * from {{ ref('stg_player_contract') }}
)

select
    player_contract_id,
    player_id,
    team_id,
    valid_from,
    valid_to,
    shirt_number,
    is_loan,
    case
        when valid_to is not null
            then valid_to - valid_from
        else current_date - valid_from
    end as contract_duration_days
from contracts
