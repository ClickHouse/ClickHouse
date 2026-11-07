-- Tags: no-parallel-replicas
-- A hash join whose serial layout `parallel_hash_join_threshold` chose keeps the probe width of the parallel layout.
-- https://github.com/ClickHouse/ClickHouse/issues/127983

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'direct,hash';
SET hash_join_layout = 'auto';
SET join_probe_resize_to_max_threads = 1;
SET parallel_hash_join_threshold = 100000;
SET collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SET max_threads = 4;
SET prefer_localhost_replica = 0;
SET query_plan_read_in_order_through_join = 0;

DROP TABLE IF EXISTS t_probe_width_dim;
CREATE TABLE t_probe_width_dim (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_width_dim SELECT number, toString(number) FROM numbers(1000);

-- Each query prints the number of `JoiningTransform` and `FillingRightJoinSide` processors (a `× N` group counts N).
-- The second tells the serial layout (1) from the parallel layout (`max_threads`). The probe side is `numbers`, a single stream.

-- The right side is estimated at 1000 rows, below the threshold: the layout is serial.
SELECT 'serial layout below threshold',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k);

-- With spilling allowed `SpillingHashJoin` wraps the join.
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'spilling serial layout below threshold',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000);

-- With the threshold at 0 the layout is parallel: the reference width.
SELECT 'parallel layout',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0);

-- An explicit serial layout is not widened: the threshold did not choose it.
SELECT 'explicit serial layout',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS hash_join_layout = 'serial');

-- The setting restores the previous width and keeps the serial layout.
SELECT 'resize disabled',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS join_probe_resize_to_max_threads = 0);

SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000, join_probe_resize_to_max_threads = 0)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'spilling resize disabled',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000, join_probe_resize_to_max_threads = 0);

-- The issue: a remote right side has no estimate, so the first run is parallel and its cached size makes the next run serial.
SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v FORMAT Null;
SELECT 'serial layout from cached stats',
    sumIf(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1), explain LIKE '%JoiningTransform%') AS joining,
    sumIf(if(explain LIKE '%FillingRightJoinSide × %', toUInt64(extract(explain, 'FillingRightJoinSide × (\\d+)')), 1), explain LIKE '%FillingRightJoinSide%') AS filling
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v);

-- `LIMIT BY` pushed into the sort above the widened join.
SET query_plan_push_limit_by_into_sort = 1;
SET optimize_sorting_by_input_stream_properties = 0;
SELECT 'limit by default join', countIf(explain LIKE '%LimitBySortedStreamTransform × 4%')
FROM (EXPLAIN PIPELINE SELECT l.number % 100 AS k, r.v AS v FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k ORDER BY k, v LIMIT 2, 3 BY k SETTINGS max_bytes_before_external_join = 1000000000);
SELECT count(), sum(cityHash64(k, v))
FROM (SELECT l.number % 100 AS k, r.v AS v FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k ORDER BY k, v LIMIT 2, 3 BY k SETTINGS max_bytes_before_external_join = 1000000000);

-- `RIGHT` and `FULL` joins emit each unmatched right row once, whatever the probe width.
SELECT count(), sum(cityHash64(r.k, r.v, l.number)) FROM numbers(500) AS l RIGHT JOIN t_probe_width_dim AS r ON l.number = r.k;
SELECT count(), sum(cityHash64(r.k, r.v, l.number)) FROM numbers(500, 1000) AS l FULL JOIN t_probe_width_dim AS r ON l.number = r.k;

-- Results do not change.
SELECT count(), sum(cityHash64(l.number, r.v)) FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k;
SELECT count(), sum(cityHash64(l.number, r.v)) FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0;

SELECT getSetting('join_probe_resize_to_max_threads') SETTINGS compatibility = '26.10';

DROP TABLE t_probe_width_dim;
