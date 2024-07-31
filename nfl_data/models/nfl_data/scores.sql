select *
from {{ source('nfl_data', 'spreadspoke_scores') }} ss
left join
    {{ source('nfl_data', 'nfl_stadiums') }} ns on ss.stadium = ns.stadium_name
