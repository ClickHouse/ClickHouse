-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

-- `UNIQUE KEY` reads drop overwritten rows through a delete bitmap, which only prunes wholly dead granules,
-- so the OFFSET-skip read-in-order optimization must not use the physical granule row counts to drop
-- leading granules.

SET enable_unique_key = 1, optimize_read_in_order = 1, enable_parallel_replicas = 0, max_threads = 1;

DROP TABLE IF EXISTS t_skip_offset_unique_key;
CREATE TABLE t_skip_offset_unique_key (id UInt64, k UInt64)
ENGINE = MergeTree ORDER BY k UNIQUE KEY id
SETTINGS index_granularity = 4, unique_key_conflict_action = 'overwrite';

INSERT INTO t_skip_offset_unique_key SELECT number, number FROM numbers(16);
INSERT INTO t_skip_offset_unique_key VALUES (0, 100);

SELECT k FROM t_skip_offset_unique_key ORDER BY k LIMIT 3 OFFSET 4 SETTINGS query_plan_optimize_read_in_order_skip_offset = 0;
SELECT k FROM t_skip_offset_unique_key ORDER BY k LIMIT 3 OFFSET 4 SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

DROP TABLE t_skip_offset_unique_key;
