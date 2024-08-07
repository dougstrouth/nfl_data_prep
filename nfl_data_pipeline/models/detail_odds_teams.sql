
WITH team_home_info AS (
SELECT
s.schedule_date,
s.schedule_season,
s.schedule_week,
s.schedule_playoff,
s.team_home,
th.team_name AS team_home_name,
s.score_home,
s.score_away,
s.team_away,
ta.team_name AS team_away_name,
s.team_favorite_id,
s.spread_favorite,
s.over_under_line,
s.stadium,
s.stadium_neutral,
s.weather_temperature,
s.weather_wind_mph,
s.weather_humidity,
s.weather_detail
FROM
{{ref('check_csv_spreadspoke_scores')}} s
LEFT JOIN
{{ ref('check_csv_nfl_teams')}} th ON s.team_home = th.team_name
LEFT JOIN
{{ ref('check_csv_nfl_teams')}} ta ON s.team_away = ta.team_name
)

SELECT
schedule_date,
schedule_season,
schedule_week,
schedule_playoff,
team_home,
team_home_name,
score_home,
score_away,
team_away,
team_away_name,
team_favorite_id,
spread_favorite,
over_under_line,
stadium,
stadium_neutral,
weather_temperature,
weather_wind_mph,
weather_humidity,
weather_detail
FROM
team_home_info
