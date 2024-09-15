{% macro get_value_distribution(schema_name) %}
    WITH column_info AS (
        {{ get_column_info(schema_name) }} -- Call the get_column_info macro
    ),
    value_distributions AS (
        {% set query_parts = [] %}
        
        {% set columns_query = "SELECT table_schema, table_name, column_name FROM column_info" %}
        {% set columns = run_query(columns_query).table %}
        
        {% for row in columns %}
            {% set table_schema = row['table_schema'] %}
            {% set table_name = row['table_name'] %}
            {% set column_name = row['column_name'] %}
            
            {% set query_part = (
                "SELECT "
                ~ "'" ~ table_schema ~ "' AS table_schema, "
                ~ "'" ~ table_name ~ "' AS table_name, "
                ~ "'" ~ column_name ~ "' AS column_name, "
                ~ column_name ~ " AS value, "
                ~ "COUNT(*) AS column_count "
                ~ "FROM " ~ table_schema ~ "." ~ table_name ~ " "
                ~ "GROUP BY " ~ column_name
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
        FROM column_info
        GROUP BY table_schema, table_name, column_name
    ) AS tc
    ON vd.table_schema = tc.table_schema
    AND vd.table_name = tc.table_name
    AND vd.column_name = tc.column_name
{% endmacro %}