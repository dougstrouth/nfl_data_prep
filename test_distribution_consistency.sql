WITH metadata AS (
    SELECT * FROM {{ ref('create_metadata_table') }}
),
distribution AS (
    SELECT * FROM {{ ref('create_distribution_table') }}
),
summary AS (
    SELECT
        d.schema,
        d.table,
        SUM(d.column_count) AS total_column_count
    FROM distribution d
    GROUP BY d.schema, d.table
),
comparison AS (
    SELECT
        m.schema,
        m.table,
        m.total_count,
        s.total_column_count,
        (m.total_count - s.total_column_count) AS diff
    FROM metadata m
    JOIN summary s
    ON m.schema = s.schema
    AND m.table = s.table
)
SELECT
    schema,
    table,
    total_count,
    total_column_count,
    diff,
    CASE
        WHEN diff = 0 THEN 'Tie out passed'
        ELSE 'Mismatch'
    END AS result
FROM comparison;