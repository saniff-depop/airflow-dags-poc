{% snapshot snp_player %}

{{
    config(
        target_schema="gold",
        unique_key="player_id",
        strategy="check",
        check_cols=["nationality", "preferred_foot"],
    )
}}

select
    player_id,
    given_name,
    family_name,
    date_of_birth,
    nationality,
    preferred_foot
from {{ source("silver", "player") }}

{% endsnapshot %}
