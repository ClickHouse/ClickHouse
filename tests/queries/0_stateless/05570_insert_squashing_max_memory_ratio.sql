-- `insert_squashing_max_memory_ratio` caps `min_insert_block_size_bytes` by a fraction of the server memory limit
-- in every squashing stage of an `INSERT`. A tiny ratio makes the cap a few bytes, so every source block of 100
-- rows is written as its own part, both into the target table and into the target of a materialized view. With
-- the cap disabled, the source blocks are squashed into one part.

DROP TABLE IF EXISTS t_squash_ratio_dst;
DROP TABLE IF EXISTS t_squash_ratio_mv;
DROP TABLE IF EXISTS t_squash_ratio;

CREATE TABLE t_squash_ratio (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_squash_ratio_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t_squash_ratio_mv TO t_squash_ratio_dst AS SELECT x FROM t_squash_ratio;

SYSTEM STOP MERGES t_squash_ratio;
SYSTEM STOP MERGES t_squash_ratio_dst;

SET max_block_size = 100, max_insert_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 1000000000,
    min_insert_block_size_rows_for_materialized_views = 0, min_insert_block_size_bytes_for_materialized_views = 0,
    deduplicate_blocks_in_dependent_materialized_views = 0;

INSERT INTO t_squash_ratio SELECT number FROM numbers(1000) SETTINGS insert_squashing_max_memory_ratio = 0.000000000001;

SELECT 'capped', table, count(), sum(rows) FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_squash_ratio', 't_squash_ratio_dst') AND active
GROUP BY table ORDER BY table;

TRUNCATE TABLE t_squash_ratio;
TRUNCATE TABLE t_squash_ratio_dst;

INSERT INTO t_squash_ratio SELECT number FROM numbers(1000) SETTINGS insert_squashing_max_memory_ratio = 0;

SELECT 'uncapped', table, count(), sum(rows) FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_squash_ratio', 't_squash_ratio_dst') AND active
GROUP BY table ORDER BY table;

-- A ratio outside of [0, 1] is rejected instead of overflowing the conversion of the cap to an integer.
INSERT INTO t_squash_ratio SELECT number FROM numbers(10) SETTINGS insert_squashing_max_memory_ratio = 1e20; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_squash_ratio SELECT number FROM numbers(10) SETTINGS insert_squashing_max_memory_ratio = -1; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_squash_ratio SELECT number FROM numbers(10) SETTINGS insert_squashing_max_memory_ratio = 1;
SELECT 'ratio 1', count() FROM t_squash_ratio;

DROP TABLE t_squash_ratio_mv;
DROP TABLE t_squash_ratio_dst;
DROP TABLE t_squash_ratio;
