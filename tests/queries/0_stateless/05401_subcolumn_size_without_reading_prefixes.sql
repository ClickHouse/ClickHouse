-- Estimating the size of a `Map(LowCardinality(String), String)` key subcolumn for PREWHERE
-- must not read the dictionaries of the parts.

SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET use_query_cache = 0;
-- Reading the statistics of the parts during planning is not what is measured.
SET use_statistics = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_subcolumn_size_lc_map;

CREATE TABLE t_subcolumn_size_lc_map (s String, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

SYSTEM STOP MERGES t_subcolumn_size_lc_map;

INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);
INSERT INTO t_subcolumn_size_lc_map SELECT toString(number % 7), map('k' || toString(number % 10), toString(number)) FROM numbers(1000);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_subcolumn_size_lc_map' AND active AND part_type = 'Wide';

SELECT countIf(explain ILIKE '%Prewhere filter%') > 0, countIf(explain ILIKE '%m.key_k1%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM t_subcolumn_size_lc_map WHERE m.key_k1 = '1' AND s = '3')
SETTINGS log_comment = '05401_subcolumn_size_without_reading_prefixes_explain';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadCompressedBytes'], ProfileEvents['LoadedMarksFiles']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05401_subcolumn_size_without_reading_prefixes_explain'
    AND type = 'QueryFinish';

DROP TABLE t_subcolumn_size_lc_map;
