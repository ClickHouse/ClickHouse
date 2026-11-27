-- `enable_join_runtime_filters_index_analysis` with a distributed query plan (`make_distributed_plan = 1`).
--
-- The worker tasks keep `use_skip_indexes_on_data_read`, so a probe side read in a shipped fragment can
-- prune granules with a JOIN runtime filter. Two plan shapes:
-- - broadcast join: the probe side is read in the fragment of the JOIN, which builds its own filter;
-- - shuffle join with `distributed_plan_join_runtime_filters = 1`: the probe side is read in a stage of its
--   own, and the filter arrives from the build stage. The task finds the `__applyFilter` of the delivered
--   filter either in the PREWHERE of the read or in a filter above it (with an explicit PREWHERE, or with
--   `optimize_move_to_prewhere = 0`).
-- A task that is not sent a filter attaches nothing for it.
--
-- The join key is pruned through the primary key of `t_dp_rf_fact`, and through a `minmax` and a `bloom_filter`
-- skip index of `t_dp_rf_minmax` and `t_dp_rf_bloom`, whose primary key does not contain it.
--
-- Every mode must return the result of a local read.

DROP TABLE IF EXISTS t_dp_rf_fact;
DROP TABLE IF EXISTS t_dp_rf_dim;
DROP TABLE IF EXISTS t_dp_rf_minmax;
DROP TABLE IF EXISTS t_dp_rf_bloom;

CREATE TABLE t_dp_rf_fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
CREATE TABLE t_dp_rf_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_dp_rf_minmax (id UInt64, v UInt64, INDEX idx_id id TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
CREATE TABLE t_dp_rf_bloom (id UInt64, v UInt64, INDEX idx_id id TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';
-- The distributed planner chooses the plan shape from row estimates, which come from the statistics
-- collected on insert.
SET materialize_statistics_on_insert = 1;
INSERT INTO t_dp_rf_fact SELECT number, number FROM numbers(100000);
INSERT INTO t_dp_rf_minmax SELECT number, number FROM numbers(100000);
INSERT INTO t_dp_rf_bloom SELECT number, number FROM numbers(100000);
INSERT INTO t_dp_rf_dim SELECT number * 50, if(number < 64, 'hot', 'cold') FROM numbers(2000);

SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
-- A `bloom_filter` index is used only while the filter keeps at most this limit / 100 exact keys (64 here).
SET join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_filter_min_probe_rows = 0;
-- Which side builds the runtime filter decides whether the probe side can be pruned at all.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
-- Distributed aggregation cannot enforce a global limit, and the functional-test profile may set one.
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 0;
-- See `materialize_statistics_on_insert` above.
SET use_statistics = 1;

SELECT 'local', count(), sum(f.v)
FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_local';

SELECT 'broadcast', count(), sum(f.v)
FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_broadcast', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 20000,
    distributed_plan_join_runtime_filters = 0;

SELECT 'shuffle_filters_sent', count(), sum(f.v)
FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_shuffle_filters_sent', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_join_runtime_filters = 1;

SELECT 'shuffle_filters_sent_explicit_prewhere', count(), sum(f.v)
FROM (SELECT * FROM t_dp_rf_fact PREWHERE v < 1000000) AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_shuffle_filters_sent_explicit_prewhere', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_join_runtime_filters = 1;

SELECT 'shuffle_filters_sent_no_move_to_prewhere', count(), sum(f.v)
FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_shuffle_filters_sent_no_move_to_prewhere', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_join_runtime_filters = 1,
    optimize_move_to_prewhere = 0;

SELECT 'shuffle_filters_not_sent', count(), sum(f.v)
FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_shuffle_filters_not_sent', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_join_runtime_filters = 0;

SELECT 'broadcast_minmax', count(), sum(f.v)
FROM t_dp_rf_minmax AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_broadcast_minmax', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 20000,
    distributed_plan_join_runtime_filters = 0;

SELECT 'shuffle_filters_sent_minmax', count(), sum(f.v)
FROM t_dp_rf_minmax AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_shuffle_filters_sent_minmax', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_join_runtime_filters = 1;

SELECT 'broadcast_bloom', count(), sum(f.v)
FROM t_dp_rf_bloom AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_broadcast_bloom', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 20000,
    distributed_plan_join_runtime_filters = 0;

SELECT 'shuffle_filters_sent_bloom', count(), sum(f.v)
FROM t_dp_rf_bloom AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05829_shuffle_filters_sent_bloom', make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_join_runtime_filters = 1;

-- The plan shapes the modes above rely on: a shuffle join reads the probe side in a stage that ends in a
-- `ShuffleSend`, a broadcast join has none.
SELECT 'broadcast: probe side read in the fragment of the join', count() = 0
FROM (
    EXPLAIN fragments = 1
    SELECT count(), sum(f.v) FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 2,
        distributed_plan_max_rows_to_broadcast = 20000
)
WHERE explain LIKE '%ShuffleSend%';

SELECT 'shuffle: probe side read in a stage of its own', count() > 0
FROM (
    EXPLAIN fragments = 1
    SELECT count(), sum(f.v) FROM t_dp_rf_fact AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 2,
        distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_shuffle_join_bucket_count = 2
)
WHERE explain LIKE '%ShuffleSend%';

SELECT 'broadcast_minmax: probe side read in the fragment of the join', count() = 0
FROM (
    EXPLAIN fragments = 1
    SELECT count(), sum(f.v) FROM t_dp_rf_minmax AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 2,
        distributed_plan_max_rows_to_broadcast = 20000
)
WHERE explain LIKE '%ShuffleSend%';

SELECT 'broadcast_bloom: probe side read in the fragment of the join', count() = 0
FROM (
    EXPLAIN fragments = 1
    SELECT count(), sum(f.v) FROM t_dp_rf_bloom AS f INNER JOIN t_dp_rf_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 2,
        distributed_plan_max_rows_to_broadcast = 20000
)
WHERE explain LIKE '%ShuffleSend%';

SYSTEM FLUSH LOGS query_log;

-- A task of a distributed plan has a `query_log` row of its own, reached through the initiator's
-- `initial_query_id`.
-- `RuntimeFilterIndexAnalysisReads` counts a read that reached its pipeline with something to prune, which
-- does not depend on scheduling. The granule counters only move once a granule is examined after the filter
-- is ready, so they are asserted only where the JOIN holds the probe side back until its build side is read:
-- a probe side read in a stage of its own can finish before its filter arrives.
SELECT
    initiator.log_comment AS mode,
    sumIf(task.ProfileEvents['RuntimeFilterIndexAnalysisReads'], NOT task.is_initial_query) > 0 AS task_read_has_descriptors,
    if(mode LIKE '05829\\_broadcast%',
        toString(sumIf(task.ProfileEvents['RuntimeFilterGranulesDropped'], NOT task.is_initial_query) > 0),
        '-') AS task_dropped_granules
FROM system.query_log AS task
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05829\\_%'
        AND event_date >= yesterday()
) AS initiator ON task.initial_query_id = initiator.query_id
WHERE task.type = 'QueryFinish' AND task.event_date >= yesterday()
    AND initiator.log_comment IN ('05829_broadcast', '05829_shuffle_filters_sent', '05829_shuffle_filters_sent_explicit_prewhere',
        '05829_shuffle_filters_sent_no_move_to_prewhere', '05829_shuffle_filters_not_sent',
        '05829_broadcast_minmax', '05829_shuffle_filters_sent_minmax', '05829_broadcast_bloom', '05829_shuffle_filters_sent_bloom')
GROUP BY mode
ORDER BY mode;

DROP TABLE t_dp_rf_fact;
DROP TABLE t_dp_rf_dim;
DROP TABLE t_dp_rf_minmax;
DROP TABLE t_dp_rf_bloom;
