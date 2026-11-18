-- Merges of ReplacingMergeTree only drop rows, so text indexes are merged
-- with remapping of row ids instead of being rebuilt for the resulting part.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_text_replacing;

CREATE TABLE t_text_replacing
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

-- Every fourth row gets replaced by the second insert, and the token 'stale'
-- exists only in the replaced rows, so it must vanish from the merged index.
INSERT INTO t_text_replacing SELECT number, 1, 'alpha row' || toString(number) || if(number % 4 = 0, ' stale', ' evergreen') FROM numbers(10000);
INSERT INTO t_text_replacing SELECT number, 2, 'fresh row' || toString(number) FROM numbers(0, 10000, 4);

OPTIMIZE TABLE t_text_replacing FINAL;

SELECT 'token of replaced rows only';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'stale') SETTINGS force_data_skipping_indices = 'idx_text';

SELECT 'tokens of surviving rows';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'evergreen') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'fresh') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'alpha') SETTINGS force_data_skipping_indices = 'idx_text';

SELECT 'exact rows after remapping';
SELECT id, text FROM t_text_replacing WHERE hasToken(text, 'row6666') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_text';
SELECT id, text FROM t_text_replacing WHERE hasToken(text, 'row6668') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT 'text indexes merged, not rebuilt';
SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_replacing' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_replacing;

-- The same scenario with the setting disabled rebuilds the index and returns the same results.
CREATE TABLE t_text_replacing
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 0;

INSERT INTO t_text_replacing SELECT number, 1, 'alpha row' || toString(number) || if(number % 4 = 0, ' stale', ' evergreen') FROM numbers(10000);
INSERT INTO t_text_replacing SELECT number, 2, 'fresh row' || toString(number) FROM numbers(0, 10000, 4);

OPTIMIZE TABLE t_text_replacing FINAL;

SELECT 'with disabled setting';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'stale') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'evergreen') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_replacing WHERE hasToken(text, 'fresh') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT 'text indexes rebuilt with disabled setting';
SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_replacing' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_replacing;

-- Cleanup merges of ReplacingMergeTree with the is_deleted column also only drop rows.
DROP TABLE IF EXISTS t_text_cleanup;

CREATE TABLE t_text_cleanup
(
    id UInt64,
    version UInt64,
    is_deleted UInt8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO t_text_cleanup SELECT number, 1, 0, 'payload row' || toString(number) FROM numbers(1000);
INSERT INTO t_text_cleanup SELECT number, 2, 1, 'tombstone row' || toString(number) FROM numbers(0, 1000, 2);

OPTIMIZE TABLE t_text_cleanup FINAL CLEANUP;

SELECT 'after cleanup merge';
SELECT count() FROM t_text_cleanup WHERE hasToken(text, 'payload') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_cleanup WHERE hasToken(text, 'tombstone') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT id FROM t_text_cleanup WHERE hasToken(text, 'row777') ORDER BY id SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT 'text indexes merged in cleanup merge';
SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_cleanup' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_text_cleanup;
