with source as (
    select * from {{ ref('stg_competition') }}
)

select
    c.competition_id,
    c.name as competition_name,
    c.country_code,
    cc.country_name,
    cc.region,
    cc.sub_region
from source as c
left join {{ ref('country_codes') }} as cc
    on c.country_code = cc.country_code
