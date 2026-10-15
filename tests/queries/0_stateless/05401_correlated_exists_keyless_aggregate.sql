-- A correlated EXISTS over an aggregate without GROUP BY is true for every outer row: the aggregate yields
-- one row (the empty-set result) also when no inner row matches the outer row.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;

DROP DICTIONARY IF EXISTS d_05401;
DROP TABLE IF EXISTS t_05401_dict_source;

CREATE TABLE t_05401_dict_source (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05401_dict_source VALUES (0, 0), (1, 10);

CREATE DICTIONARY d_05401 (id UInt64, val UInt64 DEFAULT 0)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_05401_dict_source' DB currentDatabase()))
LIFETIME(0)
LAYOUT(FLAT());

SELECT '-- a set in the subquery';
SELECT o.number, EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1))
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, NOT EXISTS (SELECT max(i.number) FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1))
FROM numbers(2) AS o ORDER BY o.number;

SELECT '-- dictGet with and without the rewrite into a set';
SELECT o.number, EXISTS (
    SELECT count() FROM numbers(2) AS i
    WHERE i.number = o.number AND dictGet(currentDatabase() || '.d_05401', 'val', i.number) >= 10)
FROM numbers(2) AS o ORDER BY o.number
SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT o.number, EXISTS (
    SELECT count() FROM numbers(2) AS i
    WHERE i.number = o.number AND dictGet(currentDatabase() || '.d_05401', 'val', i.number) >= 10)
FROM numbers(2) AS o ORDER BY o.number
SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT '-- a step above the aggregate';
SELECT o.number, EXISTS (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING c = 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT * FROM (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1) WHERE c = 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 UNION ALL SELECT 1 WHERE 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1) LIMIT 1)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1) LIMIT 1 OFFSET 1)
FROM numbers(2) AS o ORDER BY o.number;

SELECT '-- no row over an empty input';
SELECT o.number, EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1) GROUP BY i.number)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (
    SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1)
    SETTINGS empty_result_for_aggregation_by_empty_set = 1)
FROM numbers(2) AS o ORDER BY o.number;

SELECT '-- a HAVING that rejects a group without rows keeps the plan without restored groups';
SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() > 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() > 0)
    FROM numbers(2) AS o)
WHERE explain ILIKE '%correlated_aggregate_input_marker%';
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() = 0)
    FROM numbers(2) AS o)
WHERE explain ILIKE '%correlated_aggregate_input_marker%';
SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() > 0 AND max(i.number) >= 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() > 0 AND max(i.number) >= 0)
    FROM numbers(2) AS o)
WHERE explain ILIKE '%correlated_aggregate_input_marker%';
SELECT o.number, l.c
FROM numbers(2) AS o
LEFT JOIN LATERAL (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING c > 0) AS l ON true
ORDER BY o.number
SETTINGS allow_experimental_lateral_join = 1, join_use_nulls = 0;
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT o.number, l.c
    FROM numbers(2) AS o
    LEFT JOIN LATERAL (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING c > 0) AS l ON true
    SETTINGS allow_experimental_lateral_join = 1, join_use_nulls = 0)
WHERE explain ILIKE '%correlated_aggregate_input_marker%';

SELECT '-- a HAVING that cannot be decided at planning restores the groups';
SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING isNaN(avg(i.number)))
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING finalizeAggregation(countState()) = 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() > rand() % 1)
FROM numbers(2) AS o ORDER BY o.number;
SELECT count() > 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT o.number, EXISTS (SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING count() > rand() % 1)
    FROM numbers(2) AS o)
WHERE explain ILIKE '%correlated_aggregate_input_marker%';
SELECT o.number, EXISTS (
    SELECT * FROM (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1) WHERE c > o.t)
FROM (SELECT number, toInt64(number) - 1 AS t FROM numbers(2)) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING isConstant(c) = 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, l.c
FROM numbers(2) AS o
INNER JOIN LATERAL (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING isConstant(c) = 0) AS l ON true
ORDER BY o.number
SETTINGS allow_experimental_lateral_join = 1;
SELECT o.number, l.c
FROM numbers(2) AS o
INNER JOIN LATERAL (
    SELECT * FROM (SELECT count() AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1) WHERE isConstant(c) = 0) AS l ON true
ORDER BY o.number
SETTINGS allow_experimental_lateral_join = 1;

SELECT '-- an always-NULL condition of an -If aggregate';
SELECT o.number, EXISTS (SELECT countIf(NULL) AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING c > 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, EXISTS (SELECT countIf(NULL) AS c FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1 HAVING c = 0)
FROM numbers(2) AS o ORDER BY o.number;
SELECT o.number, l.c, l.s
FROM numbers(2) AS o
INNER JOIN LATERAL (
    SELECT countIf(NULL) AS c, sumIf(i.number, i.number > 0) AS s FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1) AS l ON true
ORDER BY o.number
SETTINGS allow_experimental_lateral_join = 1;

SELECT '-- layouts of the decorrelated plan';
SELECT
    o.number,
    EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1)),
    EXISTS (SELECT count() AS c FROM numbers(3) AS i WHERE i.number > o.number HAVING c = 0),
    EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number > o.number HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number;
SELECT
    o.number,
    EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1)),
    EXISTS (SELECT count() AS c FROM numbers(3) AS i WHERE i.number > o.number HAVING c = 0),
    EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number > o.number HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS correlated_subqueries_default_join_kind = 'left';
SELECT
    o.number,
    EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1)),
    EXISTS (SELECT count() AS c FROM numbers(3) AS i WHERE i.number > o.number HAVING c = 0),
    EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number > o.number HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;
SELECT
    o.number,
    EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1)),
    EXISTS (SELECT count() AS c FROM numbers(3) AS i WHERE i.number > o.number HAVING c = 0),
    EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number > o.number HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT
    o.number,
    EXISTS (SELECT count() FROM numbers(2) AS i WHERE i.number = o.number AND i.number IN (SELECT 1)),
    EXISTS (SELECT count() AS c FROM numbers(3) AS i WHERE i.number > o.number HAVING c = 0),
    EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number > o.number HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT '-- the restored groups do not count against max_rows_to_group_by';
SELECT o.number, EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number = o.number AND i.number IN (SELECT 1) HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'throw';
SELECT o.number, EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number = o.number AND i.number IN (SELECT 1) HAVING count() > 0)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';

SELECT o.number, EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number = o.number AND i.number IN (SELECT 1))
FROM numbers(3) AS o ORDER BY o.number
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'throw';
SELECT o.number, EXISTS (SELECT count() FROM numbers(3) AS i WHERE i.number = o.number AND i.number IN (SELECT 1))
FROM numbers(3) AS o ORDER BY o.number
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any';

SELECT '-- two UNION arms that read the outer rows';
SELECT o.number, EXISTS (
    SELECT 1 FROM numbers(2) AS i WHERE i.number = o.number AND i.number = 1
    UNION ALL
    SELECT 1 FROM numbers(2) AS j WHERE j.number = o.number + 1)
FROM numbers(3) AS o ORDER BY o.number
SETTINGS correlated_subqueries_use_in_memory_buffer = 0, correlated_subqueries_substitute_equivalent_expressions = 0;

DROP DICTIONARY d_05401;
DROP TABLE t_05401_dict_source;
