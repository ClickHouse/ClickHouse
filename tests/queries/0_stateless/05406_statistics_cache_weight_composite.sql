-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the test drops the server-wide statistics cache and measures its size.
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

-- The weight of a statistics cache entry includes the values nested in composite `Field`s: the `basic` statistics
-- of a `Tuple(FixedString(N))` column retain the default value of the column, a tuple with a string of N bytes.
-- `id` has statistics as well, so its condition must not prune parts by min/max: every part loads the statistics of `t`,
-- and the total weight is checked: the entries of `t` in the 10 parts weigh at least 10 * N.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The statistics are loaded to reorder the PREWHERE conditions.
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET automatic_parallel_replicas_mode = 0;
SET min_marks_per_part_to_load_statistics = 0;
SET allow_suspicious_fixed_string_types = 1;

DROP TABLE IF EXISTS t_05406;

CREATE TABLE t_05406 (id UInt64, t Tuple(FixedString(65536)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic';

SYSTEM STOP MERGES t_05406;
INSERT INTO t_05406 SELECT number, tuple('') FROM numbers(10) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

SYSTEM DROP STATISTICS CACHE;

SELECT count() FROM t_05406 WHERE t = tuple('a') AND id % 2 = 0 FORMAT Null;

WITH (SELECT value FROM system.metrics WHERE metric = 'StatisticsCacheCells') AS cells
SELECT
    cells > 0 AS loaded,
    (SELECT value FROM system.metrics WHERE metric = 'StatisticsCacheBytes') >= 10 * 65536 AS weight_includes_nested_string;

DROP TABLE t_05406;
