
-- Probe rows sorted by the join key: the runtime filter passes or drops whole blocks in runs of 5.
-- A run of passing blocks must not switch the filter off, since it drops half of the rows overall.

CREATE TABLE probe (key UInt32) ENGINE MergeTree ORDER BY tuple() SETTINGS index_granularity = 10;
CREATE TABLE build (key UInt32) ENGINE MergeTree ORDER BY tuple();

INSERT INTO probe SELECT intDiv(number, 10) FROM numbers(1000);
INSERT INTO build SELECT number FROM numbers(100) WHERE intDiv(number, 5) % 2 = 1;

SET enable_join_runtime_filters = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_size_from_hash_table_stats = 0;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET join_runtime_filter_exact_values_limit = 100;

SELECT count()
FROM probe, build
WHERE probe.key = build.key
SETTINGS max_block_size = 10, max_threads = 1, log_comment = '05620_sorted';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['RuntimeFilterBlocksSkipped'],
    ProfileEvents['RuntimeFilterRowsPassed']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish' AND log_comment = '05620_sorted'
    AND current_database = currentDatabase();
