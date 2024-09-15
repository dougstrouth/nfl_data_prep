{% macro count_records(schema_name) %}

    {% set query_parts = [] %}
    
    {% set column_info_query = get_column_info(schema_name) %}
    {% set columns = run_query(column_info_query) %}
    
    {% for row in columns %}
        {% set table_name = row['table_name'] %}
        {% if table_name not in query_parts %}
            {% set query_part = (
                "SELECT "
                ~ "'" ~ schema_name ~ "' AS table_schema, "
                ~ "'" ~ table_name ~ "' AS table_name, "
                ~ "COUNT(*) AS total_count "
                ~ "FROM " ~ schema_name ~ "." ~ table_name
            ) %}
            {% do query_parts.append(query_part) %}
        {% endif %}
    {% endfor %}
    
    {% set combined_query = query_parts | join(' UNION ALL ') %}
    
    {{ combined_query }}
    
{% endmacro %}