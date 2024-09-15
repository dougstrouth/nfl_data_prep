-- models/get_table_names.sql
{{ config(
    materialized='view'
) }}

SELECT DISTINCT table_name, table_schema,
FROM information_schema.columns
WHERE table_schema = '{{ 'play_by_play' }}'