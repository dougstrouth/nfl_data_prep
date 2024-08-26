{% macro create_table_listing(schema_name) %}
    WITH metadata AS (
        SELECT * FROM information_schema.tables where table_schema = '{{ schema_name }}';
    )
    SELECT * FROM metadata
{% endmacro %}