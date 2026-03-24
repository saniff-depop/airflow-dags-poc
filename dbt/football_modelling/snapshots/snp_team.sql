{% snapshot snp_team %}

{{
    config(
        target_schema="gold",
        unique_key="team_id",
        strategy="check",
        check_cols=[
            "team_name",
            "short_name",
            "venue_id",
            "venue_name",
            "venue_city",
            "venue_capacity",
        ],
    )
}}

select
    t.team_id,
    t.name as team_name,
    t.short_name,
    t.founded_year,
    t.venue_id,
    v.name as venue_name,
    v.city as venue_city,
    v.capacity as venue_capacity
from {{ source("silver", "team") }} as t
left join {{ source("silver", "venue") }} as v
    on t.venue_id = v.venue_id

{% endsnapshot %}
