-- Merges of CollapsingMergeTree and VersionedCollapsingMergeTree only drop rows,
-- so text indexes are merged with remapping of row ids instead of being rebuilt.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_text_collapsing;

CREATE TABLE t_text_collapsing
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

-- Every third row is cancelled by the second insert.
INSERT INTO t_text_collapsing SELECT number, 1, 'state row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_collapsing SELECT number, -1, 'state row' || toString(number) FROM numbers(0, 3000, 3);

OPTIMIZE TABLE t_text_collapsing FINAL;

SELECT 'collapsing: counts after merge';
SELECT count() FROM t_text_collapsing;
SELECT count() FROM t_text_collapsing WHERE hasToken(text, 'state') SETTINGS force_data_skipping_indices = 'idx_text';

SELECT 'collapsing: cancelled and surviving rows';
SELECT count() FROM t_text_collapsing WHERE hasToken(text, 'row2999') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_collapsing WHERE hasToken(text, 'row2997') SETTINGS force_data_skipping_indices = 'idx_text';

SELECT 'collapsing: tokens of cancelled rows are dropped from the index';
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_text_collapsing, idx_text) WHERE token IN ('row0', 'row2997');
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_text_collapsing, idx_text) WHERE token IN ('row1', 'row2999');

SYSTEM FLUSH LOGS part_log;

SELECT 'collapsing: text indexes merged, not rebuilt';
SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_collapsing' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE IF EXISTS t_text_collapsing_rebuilt;

CREATE TABLE t_text_collapsing_rebuilt
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 0;

INSERT INTO t_text_collapsing_rebuilt SELECT number, 1, 'state row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_collapsing_rebuilt SELECT number, -1, 'state row' || toString(number) FROM numbers(0, 3000, 3);

OPTIMIZE TABLE t_text_collapsing_rebuilt FINAL;

SELECT 'collapsing: merged index equals rebuilt index';
SELECT
    (SELECT groupArray((token, row_id)) FROM (SELECT token, row_id FROM mergeTreeTextIndex(currentDatabase(), t_text_collapsing, idx_text) ORDER BY token, row_id))
    =
    (SELECT groupArray((token, row_id)) FROM (SELECT token, row_id FROM mergeTreeTextIndex(currentDatabase(), t_text_collapsing_rebuilt, idx_text) ORDER BY token, row_id));

DROP TABLE t_text_collapsing;
DROP TABLE t_text_collapsing_rebuilt;

DROP TABLE IF EXISTS t_text_versioned;

CREATE TABLE t_text_versioned
(
    id UInt64,
    sign Int8,
    version UInt8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_text_versioned SELECT number, 1, 1, 'state row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_versioned SELECT number, -1, 1, 'state row' || toString(number) FROM numbers(0, 3000, 3);

OPTIMIZE TABLE t_text_versioned FINAL;

SELECT 'versioned collapsing: counts after merge';
SELECT count() FROM t_text_versioned;
SELECT count() FROM t_text_versioned WHERE hasToken(text, 'state') SETTINGS force_data_skipping_indices = 'idx_text';

SELECT 'versioned collapsing: cancelled and surviving rows';
SELECT count() FROM t_text_versioned WHERE hasToken(text, 'row2999') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_versioned WHERE hasToken(text, 'row2997') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT 'versioned collapsing: tokens of cancelled rows are dropped from the index';
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_text_versioned, idx_text) WHERE token IN ('row0', 'row2997');
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_text_versioned, idx_text) WHERE token IN ('row1', 'row2999');

SYSTEM FLUSH LOGS part_log;

SELECT 'versioned collapsing: text indexes merged, not rebuilt';
SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_text_versioned' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE IF EXISTS t_text_versioned_rebuilt;

CREATE TABLE t_text_versioned_rebuilt
(
    id UInt64,
    sign Int8,
    version UInt8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 0;

INSERT INTO t_text_versioned_rebuilt SELECT number, 1, 1, 'state row' || toString(number) FROM numbers(3000);
INSERT INTO t_text_versioned_rebuilt SELECT number, -1, 1, 'state row' || toString(number) FROM numbers(0, 3000, 3);

OPTIMIZE TABLE t_text_versioned_rebuilt FINAL;

SELECT 'versioned collapsing: merged index equals rebuilt index';
SELECT
    (SELECT groupArray((token, row_id)) FROM (SELECT token, row_id FROM mergeTreeTextIndex(currentDatabase(), t_text_versioned, idx_text) ORDER BY token, row_id))
    =
    (SELECT groupArray((token, row_id)) FROM (SELECT token, row_id FROM mergeTreeTextIndex(currentDatabase(), t_text_versioned_rebuilt, idx_text) ORDER BY token, row_id));

DROP TABLE t_text_versioned;
DROP TABLE t_text_versioned_rebuilt;
