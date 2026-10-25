-- Tags: distributed

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET min_count_to_compile_expression = 0;
SET compile_expressions = 0;

DROP TABLE IF EXISTS t_bf_lambda_interpreted;
DROP TABLE IF EXISTS t_bf_lambda_compiled;
DROP TABLE IF EXISTS t_bf_lambda_unsafe_context;

-- Two arithmetic operations make the lambda body eligible for JIT. The outer
-- arrayMap stays uncompiled, so Bloom must inspect the original lambda body.
CREATE TABLE t_bf_lambda_interpreted
(
    id UInt64,
    a Array(UInt64),
    INDEX idx_lambda arrayMap(v -> v + 1 + 2, a) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_lambda_interpreted SELECT number + 1, [number] FROM numbers(16);

SELECT 'interpreted index, interpreted query prunes 1/16',
       countIf(explain LIKE '%Name: idx_lambda%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_lambda_interpreted WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]));

SET compile_expressions = 1;

SELECT 'interpreted index, compiled query prunes 1/16',
       countIf(explain LIKE '%Name: idx_lambda%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_lambda_interpreted WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_interpreted WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]) SETTINGS use_skip_indexes = 0, compile_expressions = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_interpreted WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]) SETTINGS compile_expressions = 0) AS interpreted,
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_interpreted WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)])) AS compiled
SELECT 'interpreted index preserves typed rows with JIT on and off', compiled = interpreted AND compiled = full_scan AND compiled = [toUInt64(4)];

-- Index construction can compile its own lambda even though the containing DAG
-- is not compiled. Exercise that direction as well.
CREATE TABLE t_bf_lambda_compiled
(
    id UInt64,
    a Array(UInt64),
    INDEX idx_lambda arrayMap(v -> v + 1 + 2, a) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_lambda_compiled SELECT number + 1, [number] FROM numbers(16);

SELECT 'compiled index, interpreted query prunes 1/16',
       countIf(explain LIKE '%Name: idx_lambda%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]) SETTINGS compile_expressions = 0);

SELECT 'compiled index, compiled query prunes 1/16',
       countIf(explain LIKE '%Name: idx_lambda%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]) SETTINGS use_skip_indexes = 0, compile_expressions = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)]) SETTINGS compile_expressions = 0) AS interpreted,
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)])) AS compiled
SELECT 'compiled index preserves typed rows with JIT on and off', compiled = interpreted AND compiled = full_scan AND compiled = [toUInt64(4)];

-- `serialize_query_plan` only ships a plan on the real remote-read path. A local
-- MergeTree SELECT with that setting does not exercise serialization.
SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET distributed_group_by_no_merge = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET log_queries_min_query_duration_ms = 0;
SET log_queries_probability = 1;
SET log_query_settings = 1;
SET log_profile_events = 1;

-- This query contains only the lambda expression: its secondary query's
-- CompiledFunctionExecute event can be attributed to the lambda body.
SELECT arrayMap(v -> v + 1 + 2, a)
FROM cluster('test_shard_localhost', currentDatabase(), t_bf_lambda_compiled)
ORDER BY id
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
         enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0,
         enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
         distributed_group_by_no_merge = 0, log_comment = '05411_serialized_lambda_jit';

-- Keep pruning separate from the JIT oracle above. The query log must show that
-- the deserialized plan read marks and that the Bloom index removed granules.
SELECT id
FROM cluster('test_shard_localhost', currentDatabase(), t_bf_lambda_compiled)
WHERE arrayMap(v -> v + 1 + 2, a) IN ([toUInt64(6)])
ORDER BY id
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
         enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0,
         enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
         distributed_group_by_no_merge = 0, log_comment = '05411_serialized_lambda_pruning';

SYSTEM FLUSH LOGS query_log;

WITH
    (SELECT argMax(query_id, event_time_microseconds)
     FROM system.query_log
     WHERE current_database = currentDatabase()
       AND log_comment = '05411_serialized_lambda_jit'
       AND type = 'QueryFinish' AND is_initial_query
       AND event_date >= yesterday() AND event_time >= now() - 600) AS wanted_initial_query_id,
    (SELECT value = 'ON' FROM system.build_options WHERE name = 'USE_EMBEDDED_COMPILER') AS jit_enabled
SELECT 'serialized plan executes lambda JIT iff build supports it',
       countIf(NOT q.is_initial_query AND q.Settings['serialize_query_plan'] = '1'
               AND q.ProfileEvents['SelectedMarks'] > 0
               AND (q.ProfileEvents['CompiledFunctionExecute'] > 0) = jit_enabled) > 0,
       (countIf(NOT q.is_initial_query AND q.Settings['serialize_query_plan'] = '1'
                AND q.ProfileEvents['SelectedMarks'] > 0
                AND q.ProfileEvents['CompiledFunctionExecute'] > 0) > 0) = jit_enabled
FROM system.query_log AS q
WHERE q.initial_query_id = wanted_initial_query_id
  AND q.type = 'QueryFinish'
  AND q.event_date >= yesterday() AND q.event_time >= now() - 600;

WITH
    (SELECT argMax(query_id, event_time_microseconds)
     FROM system.query_log
     WHERE current_database = currentDatabase()
       AND log_comment = '05411_serialized_lambda_pruning'
       AND type = 'QueryFinish' AND is_initial_query
       AND event_date >= yesterday() AND event_time >= now() - 600) AS wanted_initial_query_id
SELECT 'serialized plan reads and Bloom-prunes marks',
       countIf(NOT q.is_initial_query AND q.Settings['serialize_query_plan'] = '1'
               AND q.ProfileEvents['SelectedMarks'] > 0) > 0,
       countIf(NOT q.is_initial_query AND q.Settings['serialize_query_plan'] = '1'
               AND q.ProfileEvents['SelectedMarks'] < q.ProfileEvents['SelectedMarksTotal']) > 0
FROM system.query_log AS q
WHERE q.initial_query_id = wanted_initial_query_id
  AND q.type = 'QueryFinish'
  AND q.event_date >= yesterday() AND q.event_time >= now() - 600;

-- A changed body must not borrow the original index. Row 3 matches v + 1 + 3,
-- whereas the original index stores 5 there rather than the queried value 6.
SELECT 'different compiled body declines index and reads 16/16',
       countIf(explain LIKE '%Name: idx_lambda%') = 0 AND countIf(explain LIKE '%Granules: 16/16%') > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 3, a) IN ([toUInt64(6)]));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 3, a) IN ([toUInt64(6)]) SETTINGS use_skip_indexes = 0, compile_expressions = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_compiled WHERE arrayMap(v -> v + 1 + 3, a) IN ([toUInt64(6)])) AS indexed
SELECT 'different compiled body preserves typed rows', indexed = full_scan AND indexed = [toUInt64(3)];

-- Inspecting the pre-JIT body must still reject an unproven conversion inside
-- the lambda, including when another component of the index could be used.
CREATE TABLE t_bf_lambda_unsafe_context
(
    id UInt64,
    s String,
    a Array(UInt64),
    INDEX idx_unsafe (s, arrayMap(v -> toFloat64(s) + v + 1, a)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_lambda_unsafe_context SELECT number + 1, toString(number), [number] FROM numbers(16);

SELECT 'compiled body keeps unsafe context guard',
       countIf(explain LIKE '%Name: idx_unsafe%') = 0 AND countIf(explain LIKE '%Granules: 16/16%') > 0
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_lambda_unsafe_context WHERE s = '3');

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_unsafe_context WHERE s = '3' SETTINGS use_skip_indexes = 0, compile_expressions = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_lambda_unsafe_context WHERE s = '3') AS indexed
SELECT 'unsafe context preserves typed rows', indexed = full_scan AND indexed = [toUInt64(4)];

-- Do not silently accept the interpreted path on a build which supports JIT.
-- The arrayMap body is the only compilable expression in this query.
SELECT arrayMap(v -> v + 1 + 2, a) FROM t_bf_lambda_compiled
    SETTINGS log_comment = '05411_lambda' FORMAT Null;
SELECT bitCount(bitNot(id)) FROM t_bf_lambda_compiled
    SETTINGS log_comment = '05411_control' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05411_lambda', '05411_control')
    GROUP BY log_comment
)
SELECT 'lambda compilation matches build support',
       (SELECT compiled FROM shapes WHERE log_comment = '05411_lambda')
       = (SELECT compiled FROM shapes WHERE log_comment = '05411_control'),
       (SELECT compiled FROM shapes WHERE log_comment = '05411_control')
       = ((SELECT value FROM system.build_options WHERE name = 'USE_EMBEDDED_COMPILER') = 'ON');

DROP TABLE t_bf_lambda_interpreted;
DROP TABLE t_bf_lambda_compiled;
DROP TABLE t_bf_lambda_unsafe_context;
