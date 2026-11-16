-- When `merge_memory_estimate_per_source_part_column` narrows a merge to two parts, plain `OPTIMIZE` must still
-- merge with the other selector settings at their defaults. `OPTIMIZE` selects with the base of one, and the
-- heuristic that lowers `max_parts_to_merge_at_once` according to how full the partition is interpolates
-- between the base and the cap: with a cap of two it gave slightly less than two, truncated to one, and no merge
-- was allowed. Unlike `05569_merge_width_memory_estimate`, this test keeps that heuristic enabled and the default
-- `min_parts_to_merge_at_once`.
--
-- `merge_selector_base = 1000` is far above what six fresh parts reach, so background merges do not take the
-- parts, and `OPTIMIZE` merges them synchronously.

SET optimize_throw_if_noop = 1;

DROP TABLE IF EXISTS t_merge_width_simple;
DROP TABLE IF EXISTS t_merge_width_stochastic;

CREATE TABLE t_merge_width_simple (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = 1000000000000, merge_selector_algorithm = 'Simple', merge_selector_base = 1000;

CREATE TABLE t_merge_width_stochastic (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = 1000000000000, merge_selector_algorithm = 'StochasticSimple', merge_selector_base = 1000;

SYSTEM STOP MERGES t_merge_width_simple;
SYSTEM STOP MERGES t_merge_width_stochastic;
INSERT INTO t_merge_width_simple SELECT number FROM numbers(6) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
INSERT INTO t_merge_width_stochastic SELECT number FROM numbers(6) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES t_merge_width_simple;
SYSTEM START MERGES t_merge_width_stochastic;

SELECT 'before', table, count() FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_merge_width_%' AND active GROUP BY table ORDER BY table;

OPTIMIZE TABLE t_merge_width_simple;
OPTIMIZE TABLE t_merge_width_stochastic;

SELECT 'capped', table, count() FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_merge_width_%' AND active GROUP BY table ORDER BY table;

SELECT sum(k), count() FROM t_merge_width_simple;
SELECT sum(k), count() FROM t_merge_width_stochastic;

DROP TABLE t_merge_width_simple;
DROP TABLE t_merge_width_stochastic;
