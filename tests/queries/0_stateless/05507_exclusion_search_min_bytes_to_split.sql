-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings: the assertions on the number of search steps depend on `merge_tree_coarse_index_granularity`
-- and on the settings under test.
-- no-random-merge-tree-settings: the tables rely on their explicit `index_granularity`.

-- The generic exclusion search over the primary key must not split mark ranges that hold less data than the
-- analysis of the split costs (https://github.com/ClickHouse/ClickHouse/issues/125812). With one-row granules
-- and a first key column that changes every four rows, the exhaustive search for a condition on the second key
-- column checks about two ranges per row. `merge_tree_generic_exclusion_search_min_bytes_to_split` stops the
-- descent at ranges of a few kilobytes; the query results do not change.

SET merge_tree_coarse_index_granularity = 8;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_small_granules;
DROP TABLE IF EXISTS t_wide_rows;
DROP TABLE IF EXISTS t_default_granularity;

CREATE TABLE t_small_granules (c1 DateTime, c2 UInt8, c3 String)
ENGINE = MergeTree ORDER BY (c1, c2, c3) SETTINGS index_granularity = 1;

-- One block, so that one part is written without a merge of thousands of one-row granules.
INSERT INTO t_small_granules
SELECT toDateTime('2026-01-01 00:00:00') + intDiv(number, 4), (number % 4) >= 2, toString(number % 97)
FROM numbers(16384)
SETTINGS max_insert_threads = 1, max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_small_granules' AND active;

SELECT 'small default', count(), sum(cityHash64(c1, c3)) FROM t_small_granules WHERE c2
SETTINGS log_comment = '05507 small default';
SELECT 'small exhaustive', count(), sum(cityHash64(c1, c3)) FROM t_small_granules WHERE c2
SETTINGS merge_tree_generic_exclusion_search_min_bytes_to_split = 0, log_comment = '05507 small exhaustive';
SELECT 'small no primary key', count(), sum(cityHash64(c1, c3)) FROM t_small_granules WHERE c2
SETTINGS use_primary_key = 0;

-- The setting bounds the analysis time only, so it applies regardless of `compatibility`.
SELECT 'small compatibility', count(), sum(cityHash64(c1, c3)) FROM t_small_granules WHERE c2
SETTINGS compatibility = '25.12', log_comment = '05507 small compatibility';

-- An explicit comparison with the second key column takes the same search.
SELECT 'small equals default', count(), sum(cityHash64(c1, c3)) FROM t_small_granules WHERE c2 = 1
SETTINGS log_comment = '05507 small equals default';
SELECT 'small equals exhaustive', count(), sum(cityHash64(c1, c3)) FROM t_small_granules WHERE c2 = 1
SETTINGS merge_tree_generic_exclusion_search_min_bytes_to_split = 0, log_comment = '05507 small equals exhaustive';

-- With the same one-row granules but rows of about 4 KiB, a range of a few marks already holds more data than
-- the threshold, so the search is as exact as the exhaustive one and still prunes.
CREATE TABLE t_wide_rows (c1 UInt32, c2 UInt8, c3 String)
ENGINE = MergeTree ORDER BY (c1, c2) SETTINGS index_granularity = 1;

INSERT INTO t_wide_rows
SELECT intDiv(number, 64), (number % 64) >= 32, repeat('x', 4096)
FROM numbers(4096)
SETTINGS max_insert_threads = 1, max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'wide default', count(), sum(length(c3)) FROM t_wide_rows WHERE c2
SETTINGS log_comment = '05507 wide default';
SELECT 'wide exhaustive', count(), sum(length(c3)) FROM t_wide_rows WHERE c2
SETTINGS merge_tree_generic_exclusion_search_min_bytes_to_split = 0, log_comment = '05507 wide exhaustive';

-- At the default `index_granularity` a single mark holds more data than the threshold, so nothing changes.
CREATE TABLE t_default_granularity (a UInt8, b UInt32)
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 8192;

INSERT INTO t_default_granularity
SELECT number % 4, intDiv(number, 4)
FROM numbers(1000000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'default granularity default', count(), sum(b) FROM t_default_granularity WHERE b BETWEEN 100000 AND 100100
SETTINGS log_comment = '05507 default granularity default';
SELECT 'default granularity exhaustive', count(), sum(b) FROM t_default_granularity WHERE b BETWEEN 100000 AND 100100
SETTINGS merge_tree_generic_exclusion_search_min_bytes_to_split = 0, log_comment = '05507 default granularity exhaustive';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['SelectedMarks'] AS selected_marks,
    ProfileEvents['IndexGenericExclusionSearchSteps'] < 1000 AS few_steps,
    ProfileEvents['IndexGenericExclusionSearchSteps'] > 10000 AS many_steps,
    ProfileEvents['IndexGenericExclusionSearchRangesBelowSplitThreshold'] > 0 AS ranges_accepted_whole
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05507 %'
ORDER BY log_comment;

DROP TABLE t_small_granules;
DROP TABLE t_wide_rows;
DROP TABLE t_default_granularity;
