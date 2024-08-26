{% macro count_records(schema_name, exclude_tables=[]) %}
    {%- set exclude_patterns = [] -%}

    {% set exclude_conditions = [] %}
    {% if exclude_tables %}
        {% for pattern in exclude_tables %}
            {% set condition = "table_name NOT LIKE '" ~ pattern ~ "'" %}
            {% do exclude_conditions.append(condition) %}
        {% endfor %}
    {% endif %}
    {% set exclude_condition = exclude_conditions | join(' AND ') %}
    
    {% set query_parts = [] %}
    {% set tables_query = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = '{{ schema_name }}' " ~ (exclude_condition | default("")) %}
    {% set tables = run_query(tables_query) %}
    
    {% for row in tables %}
        {% set table_name = row['table_name'] %}
        {% set query_part = (
            "SELECT "
            + "'{{ schema_name}}' AS table_schema, "
            + "'{{ table_name }}' AS table_name, "
            + "COUNT(*) AS total_count "
            + "FROM {{ schema_name }}.{{ table_name }}"
        ) %}
        {% do query_parts.append(query_part) %}
    {% endfor %}
    
    {% set combined_query = query_parts | join(' UNION ALL ') %}
    
    {{ combined_query }}
{% endmacro %}