with source as (
    select * from {{ source('silver', 'competition') }}
)

select
    competition_id,
    name,
    country_code
from source
