{% macro get_column_info(schema_name) %}
    WITH column_data AS (
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = '{{ schema_name }}'
    )
    SELECT DISTINCT
        table_schema,
        table_name,
        column_name
    FROM column_data
{% endmacro %}