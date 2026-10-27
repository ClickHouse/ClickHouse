-- Tags: no-random-settings, no-parallel-replicas
-- A `hash` join chosen over `parallel_hash` by `parallel_hash_join_threshold` must keep the probe
-- width the `parallel_hash` join would have had: the probe side is resized to `max_threads`.
-- https://github.com/ClickHouse/ClickHouse/issues/127983

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'parallel_hash,hash';
SET parallel_hash_join_threshold = 100000;
SET collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SET max_threads = 4;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_probe_width_dim;
CREATE TABLE t_probe_width_dim (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_width_dim SELECT number, toString(number) FROM numbers(1000);

-- Sum of the JoiningTransform processors in the pipeline (a `× N` group counts N).
-- The probe side is `numbers()`, a single stream.

-- The right side has an estimate of 1000 rows, below the threshold: `hash` is chosen.
SELECT 'hash below threshold', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k)
WHERE explain LIKE '%JoiningTransform%';

-- The same with spilling allowed: `SpillingHashJoin` wraps the single-threaded build.
SELECT 'spilling hash below threshold', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS max_bytes_before_external_join = 1000000000)
WHERE explain LIKE '%JoiningTransform%';

-- With the threshold at 0 the same query uses `parallel_hash`: the reference width.
SELECT 'parallel_hash', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0)
WHERE explain LIKE '%JoiningTransform%';

-- An explicit `join_algorithm = 'hash'` is not affected: nothing chose it over `parallel_hash`.
SELECT 'explicit hash', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS join_algorithm = 'hash')
WHERE explain LIKE '%JoiningTransform%';

-- The case from the issue: the right side is a remote read with no estimate, so the first run uses
-- `parallel_hash`, and its cached build size makes the next run pick `hash`. A `String` key, so the
-- build uses a two-level map, which is when `ConcurrentHashJoin` records its size.
SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v FORMAT Null;
SELECT 'hash from cached stats', sum(if(explain LIKE '%JoiningTransform × %', toUInt64(extract(explain, 'JoiningTransform × (\\d+)')), 1))
FROM (EXPLAIN PIPELINE SELECT count() FROM numbers(1000000) AS l JOIN (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_probe_width_dim)) AS r ON toString(l.number) = r.v)
WHERE explain LIKE '%JoiningTransform%';

-- Results do not change.
SELECT count(), sum(cityHash64(l.number, r.v)) FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k;
SELECT count(), sum(cityHash64(l.number, r.v)) FROM numbers(1000000) AS l JOIN t_probe_width_dim AS r ON l.number = r.k SETTINGS parallel_hash_join_threshold = 0;

DROP TABLE t_probe_width_dim;
