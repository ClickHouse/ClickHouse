-- Parts are skipped by the dictionaries of `LowCardinality` columns right before they are read: the dictionaries are not
-- read for the parts that the query condition cache excludes, or that a query with `LIMIT` does not reach, and a part
-- skipped by them is recorded in the query condition cache. Without `use_skip_indexes_on_data_read`, index analysis skips
-- them, also after the query condition cache.

SET use_low_cardinality_dictionaries_for_part_pruning = 1;
SET max_low_cardinality_dictionaries_bytes_for_part_pruning = 1048576;
-- The parts are small, so allow reading the dictionaries for any number of marks to read.
SET min_marks_per_part_to_load_statistics = 0;
SET optimize_use_implicit_projections = 0;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS t_lc_on_read;
CREATE TABLE t_lc_on_read (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1, auto_statistics_types = '';

SYSTEM STOP MERGES t_lc_on_read;
-- One part per insert.
SET max_block_size = 65536, min_insert_block_size_rows = 65536, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_insert_block_size = 65536;
INSERT INTO t_lc_on_read SELECT number, 'p0' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 100, 'p1' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 200, 'p2' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 300, 'p3' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 400, 'p4' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 500, 'p5' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 600, 'p6' FROM numbers(100);
INSERT INTO t_lc_on_read SELECT number + 700, 'p7' FROM numbers(100);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lc_on_read' AND active;

-- Every part is checked once, while it is read.
SELECT 'on read', count() FROM t_lc_on_read WHERE s = 'p3' SETTINGS log_comment = 'lc_on_read_on_read';

-- The first part that the query reads is enough.
SELECT 'limit', count() FROM (SELECT id FROM t_lc_on_read WHERE s != 'zzz' LIMIT 1) SETTINGS max_threads = 1, log_comment = 'lc_on_read_limit';

-- The second query does not read the dictionaries of the parts that the first one skipped by them.
SELECT 'cache', count() FROM t_lc_on_read WHERE s = 'p5' SETTINGS use_query_condition_cache = 1, log_comment = 'lc_on_read_cache_1';
SELECT 'cache', count() FROM t_lc_on_read WHERE s = 'p5' SETTINGS use_query_condition_cache = 1, log_comment = 'lc_on_read_cache_2';

-- Index analysis skips the parts if the read does not.
SELECT 'analysis', count() FROM t_lc_on_read WHERE s = 'p6' SETTINGS use_skip_indexes_on_data_read = 0, log_comment = 'lc_on_read_analysis';
SELECT 'analysis cache', count() FROM t_lc_on_read WHERE s = 'p7' SETTINGS use_skip_indexes_on_data_read = 0, use_query_condition_cache = 1, log_comment = 'lc_on_read_analysis_cache_1';
SELECT 'analysis cache', count() FROM t_lc_on_read WHERE s = 'p7' SETTINGS use_skip_indexes_on_data_read = 0, use_query_condition_cache = 1, log_comment = 'lc_on_read_analysis_cache_2';

SYSTEM FLUSH LOGS query_log;

-- Index analysis can run more than once for a query, so its checks are not counted. A part read by several tasks can be
-- checked more than once, so the checks on data read are compared with the number of parts.
SELECT
    log_comment,
    if(log_comment = 'lc_on_read_limit', -1, read_rows < 800) AS fewer_rows_than_all,
    multiIf(log_comment LIKE 'lc_on_read_analysis%' OR log_comment = 'lc_on_read_limit', -1,
        log_comment = 'lc_on_read_cache_2', ProfileEvents['LowCardinalityDictionariesCheckedParts'] BETWEEN 1 AND 7,
        ProfileEvents['LowCardinalityDictionariesCheckedParts'] >= 8) AS checked,
    if(log_comment = 'lc_on_read_limit', -1, ProfileEvents['LowCardinalityDictionariesPrunedParts'] > 0) AS pruned,
    if(log_comment = 'lc_on_read_limit', ProfileEvents['LowCardinalityDictionariesCheckedParts'] < 8, -1) AS checked_fewer_than_all
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'lc_on_read_%'
ORDER BY event_time_microseconds;

DROP TABLE t_lc_on_read;
