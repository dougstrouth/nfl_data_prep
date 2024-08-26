{% macro get_value_distribution(schema_name) %}
    WITH column_info AS (
        {{ get_column_info(schema_name) }}
    ),
    value_distributions AS (
        {% set query_parts = [] %}
        {% set columns_query = "SELECT table_schema, table_name, column_name FROM column_info" %}
        {% set columns = run_query(columns_query) %}
        
        {% for row in columns %}
            {% set schema_name = row['table_schema'] %}
            {% set table_name = row['table_name'] %}
            {% set column_name = row['column_name'] %}
            {% set query_part = (
                "SELECT "
                + "'{{ schema_name }}' AS table_schema, "
                + "'{{ table_name }}' AS table_name, "
                + "'{{ column_name }}' AS column_name, "
                + "value AS value, "
                + "COUNT(*) AS column_count "
                + "FROM {{ schema_name }}.{{ table_name }} "
                + "CROSS JOIN UNNEST(ARRAY_AGG(DISTINCT {{ column_name }})) AS value "
                + "GROUP BY value "
            ) %}
            {% do query_parts.append(query_part) %}
        {% endfor %}
        
        {% set combined_query = query_parts | join(' UNION ALL ') %}
        {{ combined_query }}
    )
    SELECT
        vd.table_schema,
        vd.table_name,
        vd.column_name,
        vd.value,
        vd.column_count,
        (vd.column_count * 1.0 / tc.total_count) AS pct
    FROM value_distributions vd
    JOIN (
        SELECT
            table_schema,
            table_name,
            column_name,
            COUNT(*) AS total_count
        FROM {{ ref('column_info') }}
        GROUP BY table_schema, table_name, column_name
    ) AS tc
    ON vd.table_schema = tc.table_schema
    AND vd.table_name = tc.table_name
    AND vd.column_name = tc.column_name
{% endmacro %}