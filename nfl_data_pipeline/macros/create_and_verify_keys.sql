{% macro create_and_verify_keys() %}

-- Step 1: Create and verify the primary key in aus_betting.nfl_teams
{% set primary_key_query %}
    ALTER TABLE {{ ref('nfl_teams') }}
    ADD CONSTRAINT pk_team_name PRIMARY KEY (team_name);
{% endset %}

-- Execute the primary key creation query
{% set primary_key_result = run_query(primary_key_query) %}

-- Step 2: Create and verify foreign keys in play_by_play schema
{% set foreign_key_tables = [
    {'schema': 'play_by_play', 'table': 'injuries', 'column': 'team'},
    {'schema': 'play_by_play', 'table': 'qbr', 'column': 'team'},
    {'schema': 'play_by_play', 'table': 'seasonal_pfr', 'column': 'team'},
    {'schema': 'play_by_play', 'table': 'weekly_pfr', 'column': 'team'},
    {'schema': 'play_by_play', 'table': 'weekly_rosters', 'column': 'team'}
] %}

{% set foreign_key_results = [] %}

{% for fk_table in foreign_key_tables %}
    {% set foreign_key_query %}
        ALTER TABLE {{ fk_table.schema }}.{{ fk_table.table }}
        ADD CONSTRAINT fk_{{ fk_table.table }}_team
        FOREIGN KEY ({{ fk_table.column }})
        REFERENCES aus_betting.nfl_teams (team_name);
    {% endset %}

    -- Execute the foreign key creation query and collect the result
    {% set fk_result = run_query(foreign_key_query) %}
    {% set foreign_key_results = foreign_key_results + [{'table': fk_table.table, 'result': fk_result.message}] %}
{% endfor %}

-- Step 3: Return a results table with the outcomes
select * from (
    select 'aus_betting.nfl_teams' as table_name, '{{ primary_key_result.message }}' as result
    union all
    {% for fk_result in foreign_key_results %}
        select '{{ fk_result.table }}' as table_name, '{{ fk_result.result }}' as result
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
) as key_results;

{% endmacro %}