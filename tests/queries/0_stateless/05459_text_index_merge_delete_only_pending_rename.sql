-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_renamed_offset;
DROP TABLE IF EXISTS t_renamed_index;

CREATE TABLE t_renamed_offset
(
    id UInt64,
    version UInt64,
    _part_offset UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

CREATE TABLE t_renamed_index
(
    id UInt64,
    version UInt64,
    _part_index UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_renamed_offset SELECT number, 1, 7, 'old row' || toString(number) FROM numbers(1000);
INSERT INTO t_renamed_offset SELECT number, 2, 7, 'new row' || toString(number) FROM numbers(0, 1000, 2);
INSERT INTO t_renamed_index SELECT number, 1, 0, 'old row' || toString(number) FROM numbers(1000);
INSERT INTO t_renamed_index SELECT number, 2, 0, 'new row' || toString(number) FROM numbers(0, 1000, 2);

SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

SET alter_sync = 0;
ALTER TABLE t_renamed_offset RENAME COLUMN _part_offset TO po;
ALTER TABLE t_renamed_index RENAME COLUMN _part_index TO pi;

SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_renamed_offset' AND active AND column = '_part_offset';
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_renamed_index' AND active AND column = '_part_index';

OPTIMIZE TABLE t_renamed_offset FINAL;
OPTIMIZE TABLE t_renamed_index FINAL;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

-- A source part that still stores a column named `_part_offset` or `_part_index` (here under a pending rename) also keeps rebuilding.
SELECT 'renamed shadowing columns';
SELECT count(), sum(po) FROM t_renamed_offset;
SELECT count(), sum(pi) FROM t_renamed_index;
SELECT count() FROM t_renamed_offset WHERE hasToken(text, 'old') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_renamed_offset WHERE hasToken(text, 'new') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_renamed_index WHERE hasToken(text, 'old') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_renamed_index WHERE hasToken(text, 'new') SETTINGS force_data_skipping_indices = 'idx_text';
SYSTEM FLUSH LOGS part_log;
SELECT 'renamed shadowing columns: text indexes rebuilt';
SELECT table, ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_renamed_offset', 't_renamed_index') AND event_type = 'MergeParts' AND error = 0
ORDER BY table;

DROP TABLE t_renamed_offset;
DROP TABLE t_renamed_index;
