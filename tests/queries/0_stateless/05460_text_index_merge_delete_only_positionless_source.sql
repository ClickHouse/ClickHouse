-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: uses DETACH PARTITION / ATTACH PARTITION

-- An attached part's text index written without `support_phrase_search` has no positions substream (`.pos`),
-- which the current definition needs, so a merge that only drops rows rebuilds the index instead of merging it.

SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_positionless_source;

CREATE TABLE t_positionless_source
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    allow_experimental_text_index_phrase_search = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_positionless_source SELECT number, 1, 'old row ' || toString(number) FROM numbers(1000);
INSERT INTO t_positionless_source SELECT number, 2, 'new row ' || toString(number) FROM numbers(0, 1000, 2);

ALTER TABLE t_positionless_source DETACH PARTITION tuple();
ALTER TABLE t_positionless_source DROP INDEX idx_text;
ALTER TABLE t_positionless_source ADD INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1) GRANULARITY 1;
ALTER TABLE t_positionless_source ATTACH PARTITION tuple();

OPTIMIZE TABLE t_positionless_source FINAL;

SELECT count() FROM t_positionless_source WHERE hasToken(text, 'old') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_positionless_source WHERE hasToken(text, 'new') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_positionless_source WHERE hasPhrase(text, 'new row') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_positionless_source WHERE hasPhrase(text, 'row new') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table = 't_positionless_source' AND event_type = 'MergeParts' AND error = 0;

DROP TABLE t_positionless_source;

-- `MODIFY COLUMN ... ALIAS` changes what an index over the alias indexes without rewriting the index files.

DROP TABLE IF EXISTS t_alias_source;

CREATE TABLE t_alias_source
(
    id UInt64,
    text String,
    a String ALIAS text,
    INDEX idx_a a TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_alias_source (id, text) VALUES (1, 'UPPER'), (2, 'UPPER');
INSERT INTO t_alias_source (id, text) VALUES (2, 'UPPER'), (3, 'UPPER');

ALTER TABLE t_alias_source MODIFY COLUMN a String ALIAS lower(text);

OPTIMIZE TABLE t_alias_source FINAL;

SELECT count() FROM t_alias_source WHERE hasToken(a, 'upper') SETTINGS force_data_skipping_indices = 'idx_a';
SELECT count() FROM t_alias_source WHERE hasToken(a, 'upper') SETTINGS use_skip_indexes = 0;

-- Source parts written before `ADD INDEX` have no index, which is built for the resulting part from the surviving rows.

DROP TABLE IF EXISTS t_unmaterialized_source;

CREATE TABLE t_unmaterialized_source
(
    id UInt64,
    version UInt64,
    text String
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 128, max_bytes_to_merge_at_max_space_in_pool = 1, merge_text_indexes_and_projections_on_delete_only_merges = 1;

INSERT INTO t_unmaterialized_source SELECT number, 1, 'old row ' || toString(number) FROM numbers(1000);
INSERT INTO t_unmaterialized_source SELECT number, 2, 'old row ' || toString(number) FROM numbers(1000);

ALTER TABLE t_unmaterialized_source ADD INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;

INSERT INTO t_unmaterialized_source SELECT number, 3, 'new row ' || toString(number) FROM numbers(0, 1000, 2);

OPTIMIZE TABLE t_unmaterialized_source FINAL;

SELECT count() FROM t_unmaterialized_source WHERE hasToken(text, 'old') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_unmaterialized_source WHERE hasToken(text, 'new') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT table, ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_alias_source', 't_unmaterialized_source') AND event_type = 'MergeParts' AND error = 0
ORDER BY table;

DROP TABLE t_alias_source;
DROP TABLE t_unmaterialized_source;
