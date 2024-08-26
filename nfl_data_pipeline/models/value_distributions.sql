 
{{ config(
    materialized='table'
) }}
-- depends_on: {{ ref('column_info') }}
WITH value_distributions AS (
    {{ get_value_distribution('play_by_play') }}
)

SELECT * FROM value_distributions