-- With empty_result_for_aggregation_by_constant_keys_on_empty_set = 0, a GROUP BY by constant keys returns a row over an empty input,
-- also when the outer query reads none of its aggregate functions.
SET empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;

CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;

SELECT 'count', count() FROM (SELECT 1, count() FROM t GROUP BY 1);
SELECT 'aggregate first', count() FROM (SELECT count(), 1 FROM t GROUP BY tuple());
SELECT 'key only', k FROM (SELECT count() AS c, 'k' AS k FROM numbers(0) GROUP BY 'k');
SELECT 'two keys', count() FROM (SELECT 1, 'k', count() FROM numbers(0) GROUP BY 1, 'k');
SELECT 'key reduced to a constant', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k', materialize('k'))
SETTINGS optimize_group_by_function_keys = 1, optimize_injective_functions_in_group_by = 1;
SELECT 'key made constant by multiIf', count() FROM (SELECT 1, count() FROM numbers(0) GROUP BY multiIf(1, 'k', toString(number)));
SELECT 'contradictory key', count() FROM (SELECT 1, count() FROM numbers(0) GROUP BY (number > 5 AND number < 3))
SETTINGS optimize_redundant_comparisons = 1;
SELECT 'expression', count() FROM (SELECT 'k' AS k, count() + 1 AS c FROM numbers(0) GROUP BY 'k');
SELECT 'totals', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k' WITH TOTALS);
SELECT 'grouping sets with a key', count() FROM (SELECT number AS n, 'k' AS k, count() AS c FROM numbers(0) GROUP BY GROUPING SETS ((number), ('k')));
SELECT 'grouping sets with ()', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY GROUPING SETS (('k'), ()));
SELECT 'grouping sets, default', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY GROUPING SETS (('k'), ()))
SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 1;
SELECT 'union all', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k' UNION ALL SELECT 'l', 0);
SELECT 'nested', count() FROM (SELECT * FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k'));
SELECT 'exists', exists(SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k');
SELECT 'in', 'k' IN (SELECT k FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k'));
SELECT 'join', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k') AS l JOIN (SELECT 'k' AS k) AS r ON l.k = r.k;
SELECT 'remote', count() FROM (SELECT 'k' AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(0)) GROUP BY 'k');

SELECT 'kept', count() FROM (EXPLAIN QUERY TREE SELECT k FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k'))
WHERE explain ILIKE '%function_name: count%';
SELECT 'removed at the default', count() FROM (EXPLAIN QUERY TREE SELECT k FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k')
SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 1) WHERE explain ILIKE '%function_name: count%';

SELECT 'non-constant key', count() FROM (SELECT x, count() FROM t GROUP BY x);
SELECT 'mixed keys', count() FROM (SELECT 'k', x, count() FROM t GROUP BY 'k', x);
SELECT 'grouping sets without a constant set', count() FROM (SELECT number AS n, count() AS c FROM numbers(0) GROUP BY GROUPING SETS ((number), ()));
SELECT 'non-empty input', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(5) GROUP BY 'k');
SELECT 'default setting', count() FROM (SELECT 'k' AS k, count() AS c FROM numbers(0) GROUP BY 'k')
SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 1;

DROP TABLE t;
