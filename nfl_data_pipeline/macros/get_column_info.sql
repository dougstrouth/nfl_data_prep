{% macro get_column_info(schema_name) %}
    WITH column_data AS (
        SELECT
            table_schema,
            table_name,
            column_name,
            CASE
                WHEN data_type IN ('integer', 'bigint', 'smallint', 'tinyint', 'float', 'double') THEN 'number'
                ELSE 'other'
            END AS column_type
        FROM information_schema.columns
        WHERE table_schema = '{{ schema_name }}'
    )
    SELECT
        table_schema,
        table_name,
        column_name
    FROM column_data
{% endmacro %}