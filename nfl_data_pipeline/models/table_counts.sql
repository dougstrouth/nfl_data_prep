 -- depends_on: {{ ref('column_info') }}
{{ config(
    materialized='table',
    unique_key='table_name'
) }}

WITH table_counts AS (
    {{ count_records('play_by_play') }}
)

SELECT * FROM table_counts