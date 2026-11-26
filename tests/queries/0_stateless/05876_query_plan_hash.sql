-- Tags: no-old-analyzer

-- Verifies the `ShapeHash` of the plan in `system.query_log.query_plan`: equal for plans that differ
-- only in their constants, and different when the plan differs. Every node carries the hash of its
-- subtree, and the root's is the plan's.
--
-- Queries are told apart by `log_comment` rather than by a marker in the query text, because a
-- marker literal would itself be part of the plan being hashed. Each group of queries is named by
-- the prefix of its comments.

DROP TABLE IF EXISTS t_plan_hash;
DROP TABLE IF EXISTS t_plan_hash_right;
DROP TABLE IF EXISTS t_plan_hash_memory;
DROP TABLE IF EXISTS t_plan_hash_memory_other;
DROP TABLE IF EXISTS t_plan_hash_names;
DROP TABLE IF EXISTS t_plan_hash_sides;

CREATE TABLE t_plan_hash (k UInt64, a UInt32, b UInt32, s String, n UInt8, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_plan_hash SELECT number, number % 97, number % 13, toString(number % 7), number % 200 FROM numbers(1000);

CREATE TABLE t_plan_hash_right (k UInt64, c UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_plan_hash_right SELECT number, number % 11 FROM numbers(500);

-- Two columns of the same type, in tables of the same schema.
CREATE TABLE t_plan_hash_memory (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t_plan_hash_memory SELECT number, number * 2 FROM numbers(100);
CREATE TABLE t_plan_hash_memory_other (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t_plan_hash_memory_other SELECT number, number * 3 FROM numbers(100);

-- Column names that concatenate to the same string: `ab` and `c`, `a` and `bc`.
CREATE TABLE t_plan_hash_names (k UInt64, ab UInt64, c UInt64, a UInt64, bc UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_plan_hash_names SELECT number, number, number, number, number FROM numbers(100);

CREATE TABLE t_plan_hash_sides (k UInt64, x Int64, y Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_plan_hash_sides SELECT number, number % 9, number % 5 FROM numbers(100);

SET log_query_plans = 1;

-- With parallel replicas, whether a query reads through the replicas can depend on its estimated
-- row count, which differs between the constants below, and the hash would rightly report a
-- different plan.
SET enable_parallel_replicas = 0;

-- Same shape, different constants: one hash per group.

-- `5`, `500` and `100000` are a `UInt8`, a `UInt16` and a `UInt32`.
SELECT count() FROM t_plan_hash WHERE a > 5 SETTINGS log_comment = 'same_width_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a > 500 SETTINGS log_comment = 'same_width_2' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a > 100000 SETTINGS log_comment = 'same_width_3' FORMAT Null;

-- The sets have different contents and sizes.
SELECT count() FROM t_plan_hash WHERE a IN (1, 2, 3) SETTINGS log_comment = 'same_in_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE a IN (1, 2, 3, 4, 5, 6) SETTINGS log_comment = 'same_in_2' FORMAT Null;

SELECT count() FROM t_plan_hash WHERE s = 'x' SETTINGS log_comment = 'same_string_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE s = 'a longer string' SETTINGS log_comment = 'same_string_2' FORMAT Null;

-- Without a cap on the limit, both qualify for the same top-K optimizations. With one, a limit below
-- it gets a `__topKFilter` the other does not, which is a different plan.
SELECT a FROM t_plan_hash ORDER BY a LIMIT 3
SETTINGS query_plan_max_limit_for_top_k_optimization = 0, log_comment = 'same_limit_1' FORMAT Null;
SELECT a FROM t_plan_hash ORDER BY a LIMIT 30 OFFSET 5
SETTINGS query_plan_max_limit_for_top_k_optimization = 0, log_comment = 'same_limit_2' FORMAT Null;

-- Parameters of an aggregate function are literals.
SELECT quantile(0.5)(a) FROM t_plan_hash SETTINGS log_comment = 'same_parameter_1' FORMAT Null;
SELECT quantile(0.9)(a) FROM t_plan_hash SETTINGS log_comment = 'same_parameter_2' FORMAT Null;

-- The join choices are pinned: left to the optimizer they can legitimately differ between the two
-- queries, for example when the statistics cache has learned the size of the hash table from the
-- first one, and the hash would rightly report a different plan.
SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k WHERE r.c > 5
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'same_join_1' FORMAT Null;
SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k WHERE r.c > 7
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'same_join_2' FORMAT Null;

-- `ntile` is a window function that is not an aggregate function, and its argument is a literal.
SELECT ntile(2) OVER (ORDER BY a) FROM t_plan_hash SETTINGS log_comment = 'same_window_1' FORMAT Null;
SELECT ntile(3) OVER (ORDER BY a) FROM t_plan_hash SETTINGS log_comment = 'same_window_2' FORMAT Null;

-- Over a `UInt8` column, `n + 5` is a `UInt16` and `n + 500` a `UInt32`: the width comes from the literal.
SELECT max(n + 5) FROM t_plan_hash SETTINGS log_comment = 'same_arithmetic_1' FORMAT Null;
SELECT max(n + 500) FROM t_plan_hash SETTINGS log_comment = 'same_arithmetic_2' FORMAT Null;

-- A literal in a lambda body.
SELECT sum(arraySum(arrayMap(x -> x + 1, range(a % 5)))) FROM t_plan_hash SETTINGS log_comment = 'same_lambda_1' FORMAT Null;
SELECT sum(arraySum(arrayMap(x -> x + 7, range(a % 5)))) FROM t_plan_hash SETTINGS log_comment = 'same_lambda_2' FORMAT Null;

-- `Map(UInt8, UInt8)` and `Map(UInt8, UInt16)`.
SELECT sum(length(mapValues(map(n, 5)))) FROM t_plan_hash SETTINGS log_comment = 'same_map_1' FORMAT Null;
SELECT sum(length(mapValues(map(n, 500)))) FROM t_plan_hash SETTINGS log_comment = 'same_map_2' FORMAT Null;

-- A lambda the JIT compiles: the compiled function is named after its literals. Without the
-- embedded compiler nothing is compiled, and the pairs hold trivially.
SELECT sum(arraySum(arrayMap(x -> x * x + 1, range(a % 5)))) FROM t_plan_hash
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'same_compiled_lambda_1' FORMAT Null;
SELECT sum(arraySum(arrayMap(x -> x * x + 7, range(a % 5)))) FROM t_plan_hash
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'same_compiled_lambda_2' FORMAT Null;

-- The same lambda, compiled and not.
SELECT sum(arraySum(arrayMap(x -> x * x + 3, range(a % 5)))) FROM t_plan_hash
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'same_compilation_1' FORMAT Null;
SELECT sum(arraySum(arrayMap(x -> x * x + 3, range(a % 5)))) FROM t_plan_hash
SETTINGS compile_expressions = 0, log_comment = 'same_compilation_2' FORMAT Null;

-- What the shards send is named after its expressions.
SELECT max(x + 1) FROM remote('127.0.0.{1,2}', currentDatabase(), t_plan_hash_memory)
SETTINGS prefer_localhost_replica = 0, log_comment = 'same_remote_1' FORMAT Null;
SELECT max(x + 7) FROM remote('127.0.0.{1,2}', currentDatabase(), t_plan_hash_memory)
SETTINGS prefer_localhost_replica = 0, log_comment = 'same_remote_2' FORMAT Null;

-- Different plans: two hashes per group.

SELECT count() FROM t_plan_hash WHERE a > 5 SETTINGS log_comment = 'different_column_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE b > 5 SETTINGS log_comment = 'different_column_2' FORMAT Null;

SELECT sum(a + 1) FROM t_plan_hash SETTINGS log_comment = 'different_function_1' FORMAT Null;
SELECT sum(a * 1) FROM t_plan_hash SETTINGS log_comment = 'different_function_2' FORMAT Null;

SELECT ntile(2) OVER (ORDER BY a) FROM t_plan_hash SETTINGS log_comment = 'different_window_1' FORMAT Null;
SELECT row_number() OVER (ORDER BY a) FROM t_plan_hash SETTINGS log_comment = 'different_window_2' FORMAT Null;

-- The body of a lambda that captures nothing, which is folded into a constant.
SELECT sum(arraySum(arrayMap(x -> x + 1, range(a % 5)))) FROM t_plan_hash SETTINGS log_comment = 'different_lambda_1' FORMAT Null;
SELECT sum(arraySum(arrayMap(x -> x * 2, range(a % 5)))) FROM t_plan_hash SETTINGS log_comment = 'different_lambda_2' FORMAT Null;

-- The body of a lambda that captures a column.
SELECT sum(arraySum(arrayMap(x -> x + b, range(a % 5)))) FROM t_plan_hash SETTINGS log_comment = 'different_lambda_capture_1' FORMAT Null;
SELECT sum(arraySum(arrayMap(x -> x * b, range(a % 5)))) FROM t_plan_hash SETTINGS log_comment = 'different_lambda_capture_2' FORMAT Null;

-- A join condition that reads both sides, evaluated by the join itself.
SELECT count() FROM t_plan_hash AS l LEFT JOIN t_plan_hash_right AS r ON l.k = r.k AND l.a < r.c
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'different_mixed_join_1' FORMAT Null;
SELECT count() FROM t_plan_hash AS l LEFT JOIN t_plan_hash_right AS r ON l.k = r.k AND l.a > r.c
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'different_mixed_join_2' FORMAT Null;

-- The aggregation keeps only the top groups of each bucket for the `ORDER BY ... LIMIT` above it.
SELECT k % 50 AS g, count() AS c FROM t_plan_hash GROUP BY g ORDER BY c DESC LIMIT 10
SETTINGS query_plan_push_down_limit = 1, query_plan_aggregation_bucket_top_k = 1, log_comment = 'different_bucket_top_k_1' FORMAT Null;
SELECT k % 50 AS g, count() AS c FROM t_plan_hash GROUP BY g ORDER BY c DESC LIMIT 10
SETTINGS query_plan_push_down_limit = 1, query_plan_aggregation_bucket_top_k = 0, log_comment = 'different_bucket_top_k_2' FORMAT Null;

-- Same-typed columns of a table that is not a `MergeTree`, and two tables of the same schema.
SELECT sum(x) FROM t_plan_hash_memory SETTINGS log_comment = 'different_memory_column_1' FORMAT Null;
SELECT sum(y) FROM t_plan_hash_memory SETTINGS log_comment = 'different_memory_column_2' FORMAT Null;
SELECT sum(x) FROM t_plan_hash_memory SETTINGS log_comment = 'different_memory_table_1' FORMAT Null;
SELECT sum(x) FROM t_plan_hash_memory_other SETTINGS log_comment = 'different_memory_table_2' FORMAT Null;

SELECT sum(ab) + sum(c) FROM t_plan_hash_names SETTINGS log_comment = 'different_concatenated_names_1' FORMAT Null;
SELECT sum(a) + sum(bc) FROM t_plan_hash_names SETTINGS log_comment = 'different_concatenated_names_2' FORMAT Null;

-- The joins that evaluate their whole condition themselves.
SELECT count() FROM t_plan_hash_sides AS l JOIN t_plan_hash_sides AS r ON l.x < r.x AND l.y > r.y
SETTINGS join_algorithm = 'inequality', log_comment = 'different_inequality_join_1' FORMAT Null;
SELECT count() FROM t_plan_hash_sides AS l JOIN t_plan_hash_sides AS r ON l.x <= r.x AND l.y > r.y
SETTINGS join_algorithm = 'inequality', log_comment = 'different_inequality_join_2' FORMAT Null;
SELECT count() FROM t_plan_hash_sides AS l LEFT JOIN t_plan_hash_sides AS r ON l.x < r.y
SETTINGS join_algorithm = 'hash', log_comment = 'different_nested_loop_join_1' FORMAT Null;
SELECT count() FROM t_plan_hash_sides AS l LEFT JOIN t_plan_hash_sides AS r ON l.x > r.y
SETTINGS join_algorithm = 'hash', log_comment = 'different_nested_loop_join_2' FORMAT Null;

-- Remote reads of two tables of the same schema.
SELECT sum(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_plan_hash_memory)
SETTINGS prefer_localhost_replica = 0, log_comment = 'different_remote_table_1' FORMAT Null;
SELECT sum(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_plan_hash_memory_other)
SETTINGS prefer_localhost_replica = 0, log_comment = 'different_remote_table_2' FORMAT Null;

-- The same query, with the optimizer choosing differently.
SELECT k FROM t_plan_hash ORDER BY k LIMIT 3 SETTINGS optimize_read_in_order = 1, log_comment = 'different_read_in_order_1' FORMAT Null;
SELECT k FROM t_plan_hash ORDER BY k LIMIT 3 SETTINGS optimize_read_in_order = 0, log_comment = 'different_read_in_order_2' FORMAT Null;

SELECT count() FROM t_plan_hash WHERE b = 5 SETTINGS use_skip_indexes = 1, log_comment = 'different_skip_index_1' FORMAT Null;
SELECT count() FROM t_plan_hash WHERE b = 5 SETTINGS use_skip_indexes = 0, log_comment = 'different_skip_index_2' FORMAT Null;

SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false', log_comment = 'different_join_algorithm_1' FORMAT Null;
SELECT sum(c) FROM t_plan_hash AS l JOIN t_plan_hash_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 'false', log_comment = 'different_join_algorithm_2' FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

SELECT
    replaceRegexpOne(log_comment, '_[0-9]+$', '') AS group_name,
    count() AS queries,
    uniqExact(JSONExtractString(toJSONString(query_plan), 'ShapeHash')) AS hashes,
    countIf(NOT match(JSONExtractString(toJSONString(query_plan), 'ShapeHash'), '^[0-9a-f]{16}$')) AS missing
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
  AND match(log_comment, '^(same|different)')
GROUP BY group_name
ORDER BY group_name;

-- Every step carries the hash of its subtree, and the root step's is the plan's.
WITH
    toJSONString(query_plan) AS plan,
    JSONExtractArrayRaw(plan, 'Nodes') AS nodes,
    arrayFirst(node -> JSONExtractString(node, 'NodeId') = JSONExtractString(plan, 'Root'), nodes) AS root
SELECT
    'shape_hash',
    length(nodes) > 1,
    arrayAll(node -> match(JSONExtractString(node, 'ShapeHash'), '^[0-9a-f]{16}$'), nodes),
    JSONExtractString(root, 'ShapeHash') = JSONExtractString(plan, 'ShapeHash')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'same_join_1';

DROP TABLE t_plan_hash;
DROP TABLE t_plan_hash_right;
DROP TABLE t_plan_hash_memory;
DROP TABLE t_plan_hash_memory_other;
DROP TABLE t_plan_hash_names;
DROP TABLE t_plan_hash_sides;
