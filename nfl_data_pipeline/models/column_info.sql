{{ config(
    materialized='table'
) }}

WITH column_info AS (
    {{ get_column_info('play_by_play') }}
)

SELECT * FROM column_info