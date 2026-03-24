with source as (
    select * from {{ source('silver', 'venue') }}
)

select
    venue_id,
    name,
    city,
    capacity
from source
