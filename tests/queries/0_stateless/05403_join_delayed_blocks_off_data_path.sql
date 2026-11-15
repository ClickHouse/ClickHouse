-- A spill-capable hash join (the default) keeps its delayed blocks stage off the data path,
-- and still returns the same rows when it does spill.

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET ast_fuzzer_runs = 0;

-- A default join that does not spill: no DelayedPorts funnel, the delayed blocks stage is still built.
SELECT countIf(explain ILIKE '%DelayedPorts%'), countIf(explain ILIKE '%DelayedJoinedBlocksTransform%')
FROM (EXPLAIN PIPELINE SELECT count(), sum(r.v) FROM (SELECT number AS k FROM numbers(1000)) AS l
    LEFT JOIN (SELECT number AS k, number AS v FROM numbers(100)) AS r ON l.k = r.k
    SETTINGS join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0.5, max_bytes_before_external_join = 0,
        grace_hash_join_initial_buckets = 1, max_threads = 4);

-- RIGHT parallel_hash keeps only the DelayedPorts that starts the parallel non-joined sources.
SELECT countIf(explain ILIKE '%DelayedPorts%')
FROM (EXPLAIN PIPELINE SELECT count(), sum(r.v) FROM (SELECT number AS k FROM numbers(1000)) AS l
    RIGHT JOIN (SELECT number AS k, number AS v FROM numbers(100)) AS r ON l.k = r.k
    SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 0, parallel_non_joined_rows_processing = 1,
        max_bytes_ratio_before_external_join = 0.5, max_bytes_before_external_join = 0, grace_hash_join_initial_buckets = 1, max_threads = 4);

-- Joins that spill, over 4 probe streams, return the same rows as in memory.
SET join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 4, max_threads = 4,
    max_rows_in_join = 0, parallel_hash_join_threshold = 0, parallel_non_joined_rows_processing = 1;
DROP VIEW IF EXISTS lhs_05403, rhs_05403;
CREATE VIEW lhs_05403 AS SELECT number AS k FROM numbers_mt(5000) SETTINGS max_block_size = 1024;
CREATE VIEW rhs_05403 AS SELECT number + 2500 AS k FROM numbers(5000) SETTINGS max_block_size = 1024;

SELECT (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 100000)
    = (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 0)
SETTINGS log_comment = 'spill_inner';
SELECT (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l LEFT JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 100000)
    = (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l LEFT JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 0)
SETTINGS log_comment = 'spill_left';
SELECT (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l RIGHT JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 100000)
    = (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l RIGHT JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 0)
SETTINGS log_comment = 'spill_right';
SELECT (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l FULL JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 100000)
    = (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l FULL JOIN rhs_05403 AS r ON l.k = r.k SETTINGS max_bytes_before_external_join = 0)
SETTINGS log_comment = 'spill_full';
SELECT (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l FULL JOIN rhs_05403 AS r ON l.k = r.k
        SETTINGS join_algorithm = 'parallel_hash', max_bytes_before_external_join = 100000)
    = (SELECT (count(), sum(cityHash64(l.k, r.k))) FROM lhs_05403 AS l FULL JOIN rhs_05403 AS r ON l.k = r.k
        SETTINGS join_algorithm = 'parallel_hash', max_bytes_before_external_join = 0)
SETTINGS log_comment = 'spill_full_parallel';

-- A LIMIT above a join that spilled stops it.
SELECT count() FROM (SELECT l.k FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k LIMIT 10)
SETTINGS max_bytes_before_external_join = 100000, log_comment = 'spill_limit';

-- A join that stays in memory closes its delayed stage after the build, so its workers never wait for a task.
-- When it spills they wait for every bucket.
SELECT count() FROM lhs_05403 AS l INNER JOIN (SELECT number AS k FROM numbers(100)) AS r ON l.k = r.k
SETTINGS max_bytes_ratio_before_external_join = 0.5, max_bytes_before_external_join = 0, log_processors_profiles = 1,
    log_comment = 'profile_in_memory';
SELECT count() FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k
SETTINGS max_bytes_before_external_join = 100000, log_processors_profiles = 1, log_comment = 'profile_spilled';

-- A UNION is not narrowed into a Concat above a join with delayed blocks: the delayed stage starts
-- only when all streams of the join are read at once.
SELECT count() FROM (SELECT l.k FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k UNION ALL SELECT l.k FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k)
SETTINGS max_bytes_before_external_join = 100000, max_streams_for_union_step = 1, max_streams_for_union_step_to_max_threads_ratio = 0,
    log_comment = 'spill_union';
SELECT countIf(explain ILIKE '%Concat%') FROM (EXPLAIN PIPELINE SELECT count() FROM
    (SELECT l.k FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k UNION ALL SELECT l.k FROM lhs_05403 AS l INNER JOIN rhs_05403 AS r ON l.k = r.k)
    SETTINGS max_bytes_before_external_join = 100000, max_streams_for_union_step = 1, max_streams_for_union_step_to_max_threads_ratio = 0);

-- Every spill_* query above did spill.
SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT log_comment, min(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0),
    min(log_comment = 'spill_limit' OR ProfileEvents['JoinDelayedJoinedTransformRowCount'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment LIKE 'spill\_%'
GROUP BY log_comment ORDER BY log_comment;

SELECT q.log_comment, countIf(p.name = 'DelayedJoinedBlocksWorkerTransform') > 0,
    sumIf(p.input_wait_elapsed_us, p.name = 'DelayedJoinedBlocksWorkerTransform') > 0
FROM system.processors_profile_log AS p
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND log_comment LIKE 'profile\_%'
) AS q ON p.query_id = q.query_id
WHERE p.event_date >= yesterday()
GROUP BY q.log_comment ORDER BY q.log_comment;

DROP VIEW lhs_05403, rhs_05403;
