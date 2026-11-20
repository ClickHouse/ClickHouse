-- A JOIN compares a Bool key and an integer key by value, as `=` does, whatever the table order.

SET allow_suspicious_low_cardinality_types = 1, query_plan_join_swap_table = 0, join_use_nulls = 0, explain_query_plan_default = 'legacy', query_plan_optimize_join_order_randomize = 0;

CREATE TABLE t_bool (k Nullable(Bool)) ENGINE = Memory;
INSERT INTO t_bool VALUES (false), (true), (NULL);
CREATE TABLE t_int (k UInt8) ENGINE = Memory;
INSERT INTO t_int VALUES (0), (1), (2), (255);

SELECT 'oracle', countIf(t_bool.k = t_int.k) FROM t_bool CROSS JOIN t_int;

SELECT 'on', count() FROM t_bool JOIN t_int ON t_bool.k = t_int.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'on reversed', count() FROM t_int JOIN t_bool ON t_bool.k = t_int.k;
SELECT 'on swap', count() FROM t_bool JOIN t_int ON t_bool.k = t_int.k SETTINGS query_plan_join_swap_table = 1;

SELECT 'hash', count() FROM t_bool JOIN t_int ON t_bool.k = t_int.k SETTINGS join_algorithm = 'hash';
SELECT 'parallel_hash', count() FROM t_bool JOIN t_int ON t_bool.k = t_int.k SETTINGS join_algorithm = 'parallel_hash';
SELECT 'grace_hash', count() FROM t_bool JOIN t_int ON t_bool.k = t_int.k SETTINGS join_algorithm = 'grace_hash';
SELECT 'full_sorting_merge', count() FROM t_bool JOIN t_int ON t_bool.k = t_int.k SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'using', count() FROM t_bool JOIN t_int USING (k) SETTINGS enable_join_runtime_filters = 0;
SELECT 'using reversed', count() FROM t_int JOIN t_bool USING (k) SETTINGS enable_join_runtime_filters = 0;

SELECT 'null-safe', count() FROM t_bool JOIN t_int ON t_bool.k <=> t_int.k;
SELECT 'right anti', count() FROM t_bool RIGHT ANTI JOIN t_int ON t_bool.k = t_int.k;
SELECT 'join_use_nulls', count() FROM t_bool LEFT JOIN t_int ON t_bool.k = t_int.k WHERE t_int.k IS NOT NULL SETTINGS join_use_nulls = 1;
SELECT 'anti rf', count() FROM t_int LEFT ANTI JOIN t_bool ON t_int.k = t_bool.k SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SELECT 'anti rf built', countIf(explain LIKE '%BuildRuntimeFilter%') > 0, countIf(position(explain, '__applyFilter(') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM t_int LEFT ANTI JOIN t_bool ON t_int.k = t_bool.k SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);
SELECT 'anti', count() FROM t_int LEFT ANTI JOIN t_bool ON t_int.k = t_bool.k SETTINGS enable_join_runtime_filters = 0;

SELECT 'lc bool', count() FROM (SELECT CAST(k, 'LowCardinality(Nullable(Bool))') AS k FROM t_bool) a JOIN t_int b ON a.k = b.k;
SELECT 'lc int', count() FROM t_bool a JOIN (SELECT toLowCardinality(k) AS k FROM t_int) b ON a.k = b.k;

SELECT 'array', count() FROM (SELECT [k] AS k FROM t_bool) a JOIN (SELECT [k] AS k FROM t_int) b ON a.k = b.k;
SELECT 'tuple', count() FROM (SELECT tuple(k, 1) AS k FROM t_bool) a JOIN (SELECT tuple(k, 1) AS k FROM t_int) b ON a.k = b.k;
SELECT 'subtype', count()
FROM (SELECT CAST((k, 1), 'Tuple(Nullable(Bool), UInt64)') AS k FROM t_bool) a
JOIN (SELECT CAST((k, 1), 'Tuple(UInt8, Int64)') AS k FROM t_int) b ON a.k = b.k;
SELECT 'named tuple', count() FROM (SELECT tuple(k, 1) AS k FROM t_bool) a JOIN (SELECT CAST((k, 1), 'Tuple(x UInt8, y UInt8)') AS k FROM t_int) b ON a.k = b.k;
SELECT 'named subtype', count()
FROM (SELECT CAST((k, 1), 'Tuple(a Nullable(Bool), b UInt64)') AS k FROM t_bool) a
JOIN (SELECT CAST((k, 1), 'Tuple(a UInt8, b Int64)') AS k FROM t_int) b ON a.k = b.k;
SELECT 'named anti rf', count() FROM (SELECT CAST((k, 1), 'Tuple(x UInt8, y UInt8)') AS k FROM t_int) a LEFT ANTI JOIN (SELECT tuple(k, 1) AS k FROM t_bool) b ON a.k = b.k SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SELECT 'named anti rf built', countIf(explain LIKE '%BuildRuntimeFilter%') > 0, countIf(position(explain, '__applyFilter(') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT CAST((k, 1), 'Tuple(x UInt8, y UInt8)') AS k FROM t_int) a LEFT ANTI JOIN (SELECT tuple(k, 1) AS k FROM t_bool) b ON a.k = b.k SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);

SET flatten_nested = 0;
CREATE TABLE t_nested_bool (n Nested(a Nullable(Bool))) ENGINE = Memory;
CREATE TABLE t_nested_int (n Nested(b UInt8)) ENGINE = Memory;
INSERT INTO t_nested_bool SELECT [tuple(k)] FROM t_bool;
INSERT INTO t_nested_int SELECT [tuple(k)] FROM t_int;
SELECT 'nested', count() FROM t_nested_bool a JOIN t_nested_int b ON a.n = b.n;
SELECT 'nested reversed', count() FROM t_nested_int a JOIN t_nested_bool b ON a.n = b.n;

SELECT 'json', count() FROM (SELECT CAST(toJSONString(map('a', k)), 'JSON(a Nullable(Bool))') AS k FROM t_bool) a JOIN (SELECT CAST(toJSONString(map('a', k)), 'JSON(a UInt8)') AS k FROM t_int) b ON a.k = b.k;
SELECT 'json reversed', count() FROM (SELECT CAST(toJSONString(map('a', k)), 'JSON(a UInt8)') AS k FROM t_int) a JOIN (SELECT CAST(toJSONString(map('a', k)), 'JSON(a Nullable(Bool))') AS k FROM t_bool) b ON a.k = b.k;

SELECT 'empty array', count() FROM (SELECT (CAST([], 'Array(Array(UInt8))'), k) AS k FROM t_bool) a JOIN (SELECT ([], k) AS k FROM t_int) b ON a.k = b.k;
SELECT 'empty array anti rf', count() FROM (SELECT ([], k) AS k FROM t_int) a LEFT ANTI JOIN (SELECT (CAST([], 'Array(Array(UInt8))'), k) AS k FROM t_bool) b ON a.k = b.k SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SELECT 'empty array anti rf built', countIf(explain LIKE '%BuildRuntimeFilter%') > 0, countIf(position(explain, '__applyFilter(') > 0) > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT ([], k) AS k FROM t_int) a LEFT ANTI JOIN (SELECT (CAST([], 'Array(Array(UInt8))'), k) AS k FROM t_bool) b ON a.k = b.k SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);

SELECT 'asof', groupArray(b.k)
FROM (SELECT 1 AS g, k AS t FROM t_bool WHERE k) a
ASOF JOIN (SELECT 1 AS g, k AS t, k FROM t_int) b ON a.g = b.g AND a.t >= b.t;

SELECT 'natural', count() FROM t_bool NATURAL JOIN t_int;

-- Reading `k` or `t_int.k` makes `t_int.k` an output of the join, whose USING cast must not become the comparison key.
SELECT 'using full', countIf(t_bool.k IS NULL), max(ignore(k)) FROM t_bool FULL JOIN t_int USING (k);
SELECT 'using full reversed', countIf(t_bool.k IS NULL), max(ignore(k)) FROM t_int FULL JOIN t_bool USING (k);
SELECT 'using left', count(), max(ignore(t_int.k)) FROM t_bool LEFT JOIN t_int USING (k);
SELECT 'using types', toTypeName(t_bool.k), toTypeName(k) FROM t_bool INNER JOIN t_int USING (k);
