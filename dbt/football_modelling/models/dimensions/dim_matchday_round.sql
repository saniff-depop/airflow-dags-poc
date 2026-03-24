with rounds as (
    select * from {{ ref('stg_matchday_round') }}
),

seasons as (
    select * from {{ ref('stg_season') }}
)

select
    r.round_id,
    r.season_id,
    s.label         as season_label,
    r.round_number,
    r.label         as round_label
from rounds r
left join seasons s on r.season_id = s.season_id
