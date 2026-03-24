with seasons as (
    select * from {{ ref('stg_season') }}
),

competitions as (
    select * from {{ ref('stg_competition') }}
)

select
    s.season_id,
    s.competition_id,
    c.name          as competition_name,
    s.label         as season_label,
    s.start_date,
    s.end_date
from seasons s
left join competitions c on s.competition_id = c.competition_id
