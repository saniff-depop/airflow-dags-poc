select fixture_id
from {{ ref('fct_match') }}
where home_goals < 0
   or away_goals < 0
