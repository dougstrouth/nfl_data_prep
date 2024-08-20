{% macro create_metadata_table(schema_name, exclude_tables=[]) %}
    {%- set exclude_tables = exclude_tables + ['metadata_%'] -%}
    {%- set exclude_tables = exclude_tables + ['create_%'] -%}
    WITH column_info AS (
        SELECT
            table_schema AS schema,
            table_name AS table,
            column_name AS column,
            CASE
                WHEN data_type IN ('integer', 'bigint', 'smallint', 'tinyint', 'float', 'double') THEN 'number'
                ELSE 'other'
            END AS column_type
        FROM information_schema.columns
        WHERE table_schema = '{{ schema_name }}'
          AND table_name NOT LIKE ANY (ARRAY[{{ exclude_tables | join(", ") }}])
    ),
    table_count AS (
        {% set non_number_columns = column_info | selectattr("column_type", "equalto", "other") | map(attribute="table") | unique %}
        {% for table in non_number_columns %}
            SELECT
                '{{ table }}' AS table_name,
                COUNT(*) AS total_count
            FROM {{ schema_name }}.{{ table }}
            UNION ALL
        {% endfor %}
        SELECT
            'dummy' AS table_name,
            0 AS total_count
    )
    SELECT
        c.schema,
        c.table,
        c.column,
        COALESCE(t.total_count, 0) AS total_count
    FROM column_info c
    LEFT JOIN table_count t
    ON c.table = t.table_name
{% endmacro %}