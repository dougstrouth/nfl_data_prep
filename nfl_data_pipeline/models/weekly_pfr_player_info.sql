{{
  config(
    materialized = 'table',
    )
}}
SELECT *
FROM {{ ref('weekly_pfr') }} wp
JOIN {{ ref('weekly_rosters') }} wr
ON wr.season = wp.season
   AND wr.week = wp.week
   AND wr.pfr_id = wp.pfr_player_id