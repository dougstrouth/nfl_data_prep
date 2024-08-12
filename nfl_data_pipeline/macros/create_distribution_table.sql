{% macro create_distribution_table(schema_name) %}
    WITH metadata AS (
        SELECT * FROM {{ schema_name }}.metadata_{{ schema_name }}
    ),
    distinct_values AS (
        {% for column_row in metadata %}
            SELECT
                '{{ column_row.schema }}' AS schema,
                '{{ column_row.table }}' AS table,
                '{{ column_row.column }}' AS column,
                value AS value,
                COUNT(*) AS column_count
            FROM {{ column_row.schema }}.{{ column_row.table }}
            CROSS JOIN UNNEST(ARRAY_AGG(DISTINCT {{ column_row.column }})) AS value
            GROUP BY value
            UNION ALL
        {% endfor %}
        SELECT
            'dummy' AS schema,
            'dummy' AS table,
            'dummy' AS column,
            'dummy' AS value,
            0 AS column_count
    ),
    result AS (
        SELECT
            d.schema,
            d.table,
            d.column,
            d.value,
            d.column_count,
            (d.column_count * 1.0 / m.total_count) AS pct
        FROM distinct_values d
        JOIN metadata m
        ON d.schema = m.schema
        AND d.table = m.table
        AND d.column = m.column
    )
    SELECT * FROM result
{% endmacro %}