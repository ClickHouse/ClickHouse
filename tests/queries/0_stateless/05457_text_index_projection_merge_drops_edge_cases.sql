-- Edge cases of merging text indexes and projections in merges that only drop rows.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;
SET enable_analyzer = 1;

SELECT 'all rows are dropped by the merge';

DROP TABLE IF EXISTS t_drops_all;

CREATE TABLE t_drops_all
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    PROJECTION p (SELECT id, _part_offset ORDER BY id)
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, deduplicate_merge_projection_mode = 'rebuild', remove_empty_parts = 0;

INSERT INTO t_drops_all SELECT number, 1, 'payload row' || toString(number) FROM numbers(2000);
INSERT INTO t_drops_all SELECT number, -1, 'payload row' || toString(number) FROM numbers(2000);

OPTIMIZE TABLE t_drops_all FINAL;

SELECT count() FROM t_drops_all;
SELECT count() FROM t_drops_all WHERE hasToken(text, 'payload');
SELECT count() FROM mergeTreeProjection(currentDatabase(), t_drops_all, p);
CHECK TABLE t_drops_all SETTINGS check_query_single_value_result = 1;

DROP TABLE t_drops_all;

SELECT 'one source part is fully dropped';

DROP TABLE IF EXISTS t_drops_part;

CREATE TABLE t_drops_part
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    PROJECTION p (SELECT id, _part_offset ORDER BY id)
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_drops_part SELECT number, 1, 'doomed row' || toString(number) FROM numbers(1000);
INSERT INTO t_drops_part SELECT number, -1, 'doomed row' || toString(number) FROM numbers(1000);
INSERT INTO t_drops_part SELECT number, 1, 'survivor row' || toString(number) FROM numbers(1000, 1000);

OPTIMIZE TABLE t_drops_part FINAL;

SELECT count() FROM t_drops_part;
SELECT count() FROM t_drops_part WHERE hasToken(text, 'doomed');
SELECT count() FROM t_drops_part WHERE hasToken(text, 'survivor');
SELECT count() FROM mergeTreeProjection(currentDatabase(), t_drops_part, p);
SELECT sum(l._part_offset = r._parent_part_offset) FROM t_drops_part l JOIN mergeTreeProjection(currentDatabase(), t_drops_part, p) r USING (id);
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_drops_part, idx_text) WHERE token = 'doomed';

DROP TABLE IF EXISTS t_drops_part_rebuilt;

CREATE TABLE t_drops_part_rebuilt
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    PROJECTION p (SELECT id, _part_offset ORDER BY id)
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 0, deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO t_drops_part_rebuilt SELECT number, 1, 'doomed row' || toString(number) FROM numbers(1000);
INSERT INTO t_drops_part_rebuilt SELECT number, -1, 'doomed row' || toString(number) FROM numbers(1000);
INSERT INTO t_drops_part_rebuilt SELECT number, 1, 'survivor row' || toString(number) FROM numbers(1000, 1000);

OPTIMIZE TABLE t_drops_part_rebuilt FINAL;

SELECT 'one source part is fully dropped: merged index equals rebuilt index';
SELECT
    (SELECT groupArray((token, row_id)) FROM (SELECT token, row_id FROM mergeTreeTextIndex(currentDatabase(), t_drops_part, idx_text) ORDER BY token, row_id))
    =
    (SELECT groupArray((token, row_id)) FROM (SELECT token, row_id FROM mergeTreeTextIndex(currentDatabase(), t_drops_part_rebuilt, idx_text) ORDER BY token, row_id));

DROP TABLE t_drops_part;
DROP TABLE t_drops_part_rebuilt;

SELECT 'incorrect sign values are detected during the merge';

DROP TABLE IF EXISTS t_invalid_sign;

CREATE TABLE t_invalid_sign
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1,
    enable_vertical_merge_algorithm = 0;

-- A row with an invalid sign is emitted by the collapsing algorithm out of source order.
-- With the offset mapping active it is detected as corrupted data instead of
-- silently producing a text index with wrong row ids.
-- An INSERT with the default optimize_on_insert = 1 validates the sign, so the invalid value is injected by attaching a part
-- prepared in a table without the validation.
-- Only the horizontal merge emits the rows out of order: the vertical merge keeps the source order and merges the
-- index correctly, so it is disabled here.
DROP TABLE IF EXISTS t_sign_source;

CREATE TABLE t_sign_source
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_sign_source VALUES (1, -1, 'first'), (1, 0, 'second');

ALTER TABLE t_invalid_sign ATTACH PARTITION tuple() FROM t_sign_source;

OPTIMIZE TABLE t_invalid_sign FINAL; -- { serverError INCORRECT_DATA }

DROP TABLE t_invalid_sign;

-- With the setting disabled the same merge rebuilds the index and succeeds.
CREATE TABLE t_invalid_sign
(
    id UInt64,
    sign Int8,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 0;

ALTER TABLE t_invalid_sign ATTACH PARTITION tuple() FROM t_sign_source;

OPTIMIZE TABLE t_invalid_sign FINAL;

SELECT count() FROM t_invalid_sign;

DROP TABLE t_invalid_sign;
DROP TABLE t_sign_source;

-- A table column named `_part_offset` or `_part_index` shadows the virtual column read by the offset mapping,
-- so such merges keep rebuilding text indexes, and the column keeps its values.
DROP TABLE IF EXISTS t_shadow_offset;
DROP TABLE IF EXISTS t_shadow_index;

CREATE TABLE t_shadow_offset
(
    id UInt64,
    version UInt64,
    _part_offset UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

CREATE TABLE t_shadow_index
(
    id UInt64,
    version UInt64,
    _part_index UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_shadow_offset SELECT number, 1, 7, 'old row' || toString(number) FROM numbers(1000);
INSERT INTO t_shadow_offset SELECT number, 2, 7, 'new row' || toString(number) FROM numbers(0, 1000, 2);
INSERT INTO t_shadow_index SELECT number, 1, 0, 'old row' || toString(number) FROM numbers(1000);
INSERT INTO t_shadow_index SELECT number, 2, 0, 'new row' || toString(number) FROM numbers(0, 1000, 2);

OPTIMIZE TABLE t_shadow_offset FINAL;
OPTIMIZE TABLE t_shadow_index FINAL;

SELECT 'shadowing columns';
SELECT count(), sum(_part_offset) FROM t_shadow_offset;
SELECT count(), sum(_part_index) FROM t_shadow_index;
SELECT count() FROM t_shadow_offset WHERE hasToken(text, 'old') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_shadow_offset WHERE hasToken(text, 'new') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_shadow_index WHERE hasToken(text, 'old') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_shadow_index WHERE hasToken(text, 'new') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT 'shadowing columns: text indexes rebuilt';
SELECT table, ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_shadow_offset', 't_shadow_index') AND event_type = 'MergeParts' AND error = 0
ORDER BY table;

DROP TABLE t_shadow_offset;
DROP TABLE t_shadow_index;
