-- Scalar subqueries evaluated under different SETTINGS scopes must not share cached values.
DROP TABLE IF EXISTS scalar_subquery_scope_settings;

CREATE TABLE scalar_subquery_scope_settings (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO scalar_subquery_scope_settings SELECT number FROM numbers(10);

-- The outer scalar must not use the inner query's additional_table_filters.
SELECT 'outer-first', (SELECT count() FROM scalar_subquery_scope_settings) AS outer_count,
    (SELECT count() + 0 FROM scalar_subquery_scope_settings) AS control, inner_count
FROM
(
    SELECT (SELECT count() FROM scalar_subquery_scope_settings) AS inner_count
    SETTINGS additional_table_filters = {'scalar_subquery_scope_settings': 'x > 5'}
);

-- Also check the reversed analysis order through a scalar SELECT.
SELECT 'inner-first', (SELECT count() FROM scalar_subquery_scope_settings) AS outer_count,
    (SELECT inner_count FROM
    (
        SELECT (SELECT count() FROM scalar_subquery_scope_settings) AS inner_count
        SETTINGS additional_table_filters = {'scalar_subquery_scope_settings': 'x > 5'}
    )) AS inner_count;

-- Arrays use __getScalar rather than being folded into a literal.
SELECT 'arrays', (SELECT arraySort(groupArray(x)) FROM scalar_subquery_scope_settings) AS outer_array, inner_array
FROM
(
    SELECT (SELECT arraySort(groupArray(x)) FROM scalar_subquery_scope_settings) AS inner_array
    SETTINGS additional_table_filters = {'scalar_subquery_scope_settings': 'x > 5'}
);

-- EXPLAIN can evaluate a scalar under its local SETTINGS scope during analysis.
SELECT 'explain', (SELECT count() FROM scalar_subquery_scope_settings) AS outer_count
FROM
(
    EXPLAIN AST optimize = 1 SELECT (SELECT count() FROM scalar_subquery_scope_settings)
    SETTINGS additional_table_filters = {'scalar_subquery_scope_settings': 'x > 5'}
)
LIMIT 1;

-- Identical scalar subqueries with the same settings still return the same value.
SELECT 'same-settings',
    (SELECT count() FROM scalar_subquery_scope_settings),
    (SELECT count() FROM scalar_subquery_scope_settings);

DROP TABLE scalar_subquery_scope_settings;
