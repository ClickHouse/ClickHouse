-- Tags: no-parallel-replicas
-- A `hash` join chosen over `parallel_hash` by `parallel_hash_join_threshold` must keep the probe
-- width the `parallel_hash` join would have had: the probe side is resized to `max_threads`.
-- https://github.com/ClickHouse/ClickHouse/issues/127983

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'direct,parallel_hash,hash';
SET parallel_hash_join_threshold = 100000;
SET collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SET max_threads = 4;
SET prefer_localhost_replica = 0;
SET query_plan_read_in_order_through_join = 0;

DROP TABLE IF EXISTS t_probe_width_dim;
CREATE TABLE t_probe_width_dim (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_width_dim SELECT number, toString(number) FROM numbers(1000);

-- Check the algorithm as well as the number of `JoiningTransform` processors
-- (a `× N` group counts N), so an unexpected `parallel_hash` cannot pass unnoticed.
-- The probe side is `numbers`, a single stream.

-- The right side has an estimate of 1000 rows, below the threshold: `hash` is chosen.
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'hash below threshold', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k)
WHERE explain LIKE '%JoiningTransform%';

-- The same with spilling allowed: `SpillingHashJoin` wraps the single-threaded build.
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'spilling hash below threshold', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000)
WHERE explain LIKE '%JoiningTransform%';

-- With the threshold at 0 the same query uses `parallel_hash`: the reference width.
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'parallel_hash', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0)
WHERE explain LIKE '%JoiningTransform%';

-- An explicit `join_algorithm = 'hash'` is not affected: nothing chose it over `parallel_hash`.
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS join_algorithm = 'hash')
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'explicit hash', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS join_algorithm = 'hash')
WHERE explain LIKE '%JoiningTransform%';

-- The compatibility switch retains the previous width in both build modes.
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS join_probe_resize_to_max_threads = 0)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'resize disabled', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS join_probe_resize_to_max_threads = 0)
WHERE explain LIKE '%JoiningTransform%';

SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000, join_probe_resize_to_max_threads = 0)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'spilling resize disabled', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000, join_probe_resize_to_max_threads = 0)
WHERE explain LIKE '%JoiningTransform%';

-- The case from the issue: the right side is a remote read with no estimate, so the first run uses
-- `parallel_hash`, and its cached build size makes the next run pick `hash`. A `String` key, so the
-- build uses a two-level map, which is when `ConcurrentHashJoin` records its size.
SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v FORMAT Null;
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'hash from cached stats', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v)
WHERE explain LIKE '%JoiningTransform%';

-- Cover the default algorithm list with `LIMIT BY` pushed into the sort above the widened join.
SET query_plan_push_limit_by_into_sort = 1;
SET optimize_sorting_by_input_stream_properties = 0;
SELECT trim(explain)
FROM (EXPLAIN PLAN actions = 1 SELECT l.number % 100 AS k, r.v AS v FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k ORDER BY k, v LIMIT 2, 3 BY k SETTINGS max_bytes_before_external_join = 1000000000)
WHERE trim(explain) LIKE 'Algorithm:%';
SELECT 'limit by default join', countIf(explain LIKE '%LimitBySortedStreamTransform × 4%')
FROM (EXPLAIN PIPELINE SELECT l.number % 100 AS k, r.v AS v FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k ORDER BY k, v LIMIT 2, 3 BY k SETTINGS max_bytes_before_external_join = 1000000000);
SELECT count(), sum(cityHash64(k, v))
FROM (SELECT l.number % 100 AS k, r.v AS v FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k ORDER BY k, v LIMIT 2, 3 BY k SETTINGS max_bytes_before_external_join = 1000000000);

-- Results do not change.
SELECT count(), sum(cityHash64(l.number, r.v)) FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k;
SELECT count(), sum(cityHash64(l.number, r.v)) FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0;

SELECT getSetting('join_probe_resize_to_max_threads') SETTINGS compatibility = '26.10';

DROP TABLE t_probe_width_dim;
