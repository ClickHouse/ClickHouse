-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel-replicas: the part aggregation cache is intentionally not used for parallel-replica reads

-- Regression test: settings captured by ordinary functions at build time (`formatDateTime`,
-- `locate`) are not visible to `ActionsDAG::updateHash`, so they must be part of the cache key.
-- Otherwise a state warmed under one setting value is reused under another.

-- See `05029_part_aggregation_cache_default_expression` for why the limits are pinned to 0.
SET allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_settings;

CREATE TABLE t_part_agg_cache_settings (d DateTime('UTC'), s String) ENGINE = MergeTree ORDER BY d;
SYSTEM STOP MERGES t_part_agg_cache_settings;
INSERT INTO t_part_agg_cache_settings VALUES ('2020-01-01 00:00:00', 'abc'), ('2020-01-01 00:00:01', 'abc');

-- Grouping key.
SELECT formatDateTime(d, '%f') AS k, count() FROM t_part_agg_cache_settings GROUP BY k ORDER BY k SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT count() > 0 FROM system.part_aggregation_cache;
SELECT formatDateTime(d, '%f') AS k, count() FROM t_part_agg_cache_settings GROUP BY k ORDER BY k SETTINGS formatdatetime_f_prints_single_zero = 0;

-- Aggregate argument.
SELECT sum(locate('a', s)) FROM t_part_agg_cache_settings GROUP BY d % 1 SETTINGS function_locate_has_mysql_compatible_argument_order = 1;
SELECT sum(locate('a', s)) FROM t_part_agg_cache_settings GROUP BY d % 1 SETTINGS function_locate_has_mysql_compatible_argument_order = 0;

-- `WHERE`.
SELECT d % 1 AS k, count() FROM t_part_agg_cache_settings WHERE locate('a', s) > 0 GROUP BY k SETTINGS function_locate_has_mysql_compatible_argument_order = 1;
SELECT d % 1 AS k, count() FROM t_part_agg_cache_settings WHERE locate('a', s) > 0 GROUP BY k SETTINGS function_locate_has_mysql_compatible_argument_order = 0;

-- `PREWHERE`.
SELECT d % 1 AS k, count() FROM t_part_agg_cache_settings PREWHERE formatDateTime(d, '%f') = '0' GROUP BY k SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT d % 1 AS k, count() FROM t_part_agg_cache_settings PREWHERE formatDateTime(d, '%f') = '0' GROUP BY k SETTINGS formatdatetime_f_prints_single_zero = 0;

DROP TABLE t_part_agg_cache_settings;
