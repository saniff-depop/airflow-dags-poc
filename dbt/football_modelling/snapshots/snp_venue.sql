{% snapshot snp_venue %}

{{
    config(
        target_schema="gold",
        unique_key="venue_id",
        strategy="check",
        check_cols=["venue_name", "capacity"],
    )
}}

select
    venue_id,
    name as venue_name,
    city,
    capacity
from {{ source("silver", "venue") }}

{% endsnapshot %}
