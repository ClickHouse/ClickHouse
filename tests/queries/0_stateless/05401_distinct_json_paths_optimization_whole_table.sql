-- The distinctJSONPaths subcolumn rewrite must give the same result as the function itself.

DROP TABLE IF EXISTS t_djp;
CREATE TABLE t_djp (key UInt64, j JSON, e JSON, arr Array(UInt8)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_djp VALUES (1, '{"a":1}', '{}', []), (2, '{"b":2}', '{}', [1]), (3, '{"a":3}', '{}', [1]), (4, '{"b":4}', '{}', [1]);

-- The rewrite still applies to a whole-table aggregate, also next to other aggregates.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT distinctJSONPaths(j), count(), sum(key) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%groupUniqArrayArray%';
SELECT distinctJSONPaths(j), count(), sum(key) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j), count(), sum(key) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j), distinctJSONPaths(e) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j), distinctJSONPaths(e) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT arrayJoin(distinctJSONPaths(j)) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%groupUniqArrayArray%';
SELECT arrayJoin(distinctJSONPaths(j)) AS p FROM t_djp ORDER BY p SETTINGS optimize_functions_to_subcolumns = 1;
SELECT arrayJoin(distinctJSONPaths(j)) AS p FROM t_djp ORDER BY p SETTINGS optimize_functions_to_subcolumns = 0;

-- Window functions.
SELECT key, distinctJSONPaths(j) OVER (PARTITION BY key) FROM t_djp ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;
SELECT key, distinctJSONPaths(j) OVER (PARTITION BY key) FROM t_djp ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j) OVER () FROM t_djp SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) OVER () FROM t_djp SETTINGS optimize_functions_to_subcolumns = 0;
SELECT k, distinctJSONPaths(j) OVER (PARTITION BY k) FROM values('k UInt8, j JSON', (1, '{"a":1}'), (2, '{"b":2}')) ORDER BY k SETTINGS optimize_functions_to_subcolumns = 1;
SELECT k, distinctJSONPaths(j) OVER (PARTITION BY k) FROM values('k UInt8, j JSON', (1, '{"a":1}'), (2, '{"b":2}')) ORDER BY k SETTINGS optimize_functions_to_subcolumns = 0;

-- Rows filtered or multiplied before the aggregate.
SELECT distinctJSONPaths(t_djp.j) FROM t_djp INNER JOIN (SELECT 2 AS k) AS s ON t_djp.key = s.k SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(t_djp.j) FROM t_djp INNER JOIN (SELECT 2 AS k) AS s ON t_djp.key = s.k SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j) FROM t_djp ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j), count(arrayJoin(arr)) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j), count(arrayJoin(arr)) FROM t_djp SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j), count(unnest(arr)) FROM t_djp SETTINGS normalize_function_names = 0, optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j), count(unnest(arr)) FROM t_djp SETTINGS normalize_function_names = 0, optimize_functions_to_subcolumns = 0;
SELECT any(key IN (SELECT 1)), distinctJSONPaths(j) FROM t_djp WHERE key = 2 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT any(key IN (SELECT 1)), distinctJSONPaths(j) FROM t_djp WHERE key = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j) FROM t_djp SETTINGS additional_table_filters = {'t_djp': 'key = 2'}, optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp SETTINGS additional_table_filters = {'t_djp': 'key = 2'}, optimize_functions_to_subcolumns = 0;

-- A read cut short by a limit in break mode: one row per granule and per block, one thread.
DROP TABLE IF EXISTS t_djp_break;
CREATE TABLE t_djp_break (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1;
INSERT INTO t_djp_break VALUES (1, '{"a":1}'), (2, '{"b":2}'), (3, '{"c":3}');
SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_block_size = 1, max_threads = 1, max_rows_to_read = 1, read_overflow_mode = 'break', optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_block_size = 1, max_threads = 1, max_rows_to_read = 1, read_overflow_mode = 'break', optimize_functions_to_subcolumns = 0;
SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_block_size = 1, max_threads = 1, max_rows_to_read_leaf = 1, read_overflow_mode_leaf = 'break', optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_block_size = 1, max_threads = 1, max_rows_to_read_leaf = 1, read_overflow_mode_leaf = 'break', optimize_functions_to_subcolumns = 0;
-- The limit of an enclosing query still applies when the subquery resets its own.
SELECT * FROM (SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw') SETTINGS enable_parallel_replicas = 0, max_block_size = 1, max_threads = 1, max_rows_to_read = 1, read_overflow_mode = 'break', optimize_functions_to_subcolumns = 1;
SELECT * FROM (SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw') SETTINGS enable_parallel_replicas = 0, max_block_size = 1, max_threads = 1, max_rows_to_read = 1, read_overflow_mode = 'break', optimize_functions_to_subcolumns = 0;
SELECT * FROM (SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_rows_to_read_leaf = 0, read_overflow_mode_leaf = 'throw') SETTINGS enable_parallel_replicas = 0, max_block_size = 1, max_threads = 1, max_rows_to_read_leaf = 1, read_overflow_mode_leaf = 'break', optimize_functions_to_subcolumns = 1;
SELECT * FROM (SELECT distinctJSONPaths(j) FROM t_djp_break SETTINGS max_rows_to_read_leaf = 0, read_overflow_mode_leaf = 'throw') SETTINGS enable_parallel_replicas = 0, max_block_size = 1, max_threads = 1, max_rows_to_read_leaf = 1, read_overflow_mode_leaf = 'break', optimize_functions_to_subcolumns = 0;

-- SAMPLE: the first row of the part is outside the sample.
DROP TABLE IF EXISTS t_djp_sample;
CREATE TABLE t_djp_sample (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key SAMPLE BY key;
INSERT INTO t_djp_sample VALUES (1, '{"a":1}'), (9223372036854775809, '{"b":2}'), (9223372036854775810, '{"c":3}');
SELECT distinctJSONPaths(j) FROM t_djp_sample SAMPLE 1/2 OFFSET 1/2 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp_sample SAMPLE 1/2 OFFSET 1/2 SETTINGS optimize_functions_to_subcolumns = 0;
-- A replica of parallel replicas over the sampling key reads the same slice as SAMPLE 1/2 OFFSET 1/2.
SELECT distinctJSONPaths(j) FROM t_djp_sample SETTINGS automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, parallel_replicas_mode = 'sampling_key', max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_count = 2, parallel_replica_offset = 1, optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp_sample SETTINGS automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, parallel_replicas_mode = 'sampling_key', max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_count = 2, parallel_replica_offset = 1, optimize_functions_to_subcolumns = 0;
-- An initiator with a custom key does not rewrite.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT distinctJSONPaths(j) FROM t_djp_sample SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'key', optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%__special_subcolumn_name_for_distinct_paths_calculation%';

-- Row policy.
DROP TABLE IF EXISTS t_djp_policy;
CREATE TABLE t_djp_policy (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_djp_policy VALUES (1, '{"a":1}'), (2, '{"b":2}'), (3, '{"c":3}');
DROP ROW POLICY IF EXISTS p_djp ON t_djp_policy;
CREATE ROW POLICY p_djp ON t_djp_policy USING key = 2 TO ALL;
SELECT distinctJSONPaths(j) FROM t_djp_policy SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp_policy SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY p_djp ON t_djp_policy;

-- A Merge table applies the row policies of its children while reading.
DROP TABLE IF EXISTS t_djp_merge;
DROP TABLE IF EXISTS t_djp_child1;
DROP TABLE IF EXISTS t_djp_child2;
CREATE TABLE t_djp_child1 (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_djp_child2 (key UInt64, j JSON) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_djp_child1 VALUES (1, '{"a":1}'), (2, '{"b":2}');
INSERT INTO t_djp_child2 VALUES (3, '{"c":3}');
CREATE TABLE t_djp_merge (key UInt64, j JSON) ENGINE = Merge(currentDatabase(), '^t_djp_child');
DROP ROW POLICY IF EXISTS p_djp_child ON t_djp_child1;
CREATE ROW POLICY p_djp_child ON t_djp_child1 USING key = 2 TO ALL;
SELECT distinctJSONPaths(j) FROM t_djp_merge SETTINGS optimize_functions_to_subcolumns = 1;
SELECT distinctJSONPaths(j) FROM t_djp_merge SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY p_djp_child ON t_djp_child1;

DROP TABLE t_djp_merge;
DROP TABLE t_djp_child1;
DROP TABLE t_djp_child2;
DROP TABLE t_djp_policy;
DROP TABLE t_djp_sample;
DROP TABLE t_djp_break;
DROP TABLE t_djp;
