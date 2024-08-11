{{
  config(
    materialized = 'table',
    )
}}
SELECT *
FROM {{ ref('seasonal_pfr') }} sp
JOIN {{ ref('weekly_rosters') }} wr
ON wr.season = sp.season
   AND wr.pfr_id = sp.pfr_id