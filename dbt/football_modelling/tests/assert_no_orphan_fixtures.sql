select f.fixture_id
from {{ ref('fct_match') }} f
left join {{ ref('dim_matchday_round') }} r on f.round_id = r.round_id
left join {{ ref('dim_team') }} ht on f.home_team_id = ht.team_id
left join {{ ref('dim_team') }} at_ on f.away_team_id = at_.team_id
left join {{ ref('dim_venue') }} v on f.venue_id = v.venue_id
where r.round_id is null
   or ht.team_id is null
   or at_.team_id is null
   or v.venue_id is null
