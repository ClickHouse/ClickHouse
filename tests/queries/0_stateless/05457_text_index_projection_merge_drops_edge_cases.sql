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

DROP TABLE t_drops_part;

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
-- INSERT validates the sign, so the invalid value is injected by attaching a part
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
