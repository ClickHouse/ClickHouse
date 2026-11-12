-- Random settings limits: merge_max_block_size=(1024, None)
-- A text index merged in a merge that drops rows must be identical to the index rebuilt for the resulting part:
-- the same tokens, posting list formats, row ids and positions.
-- Tokens are chosen so that the merge removes all postings of some of them, shrinks large posting lists
-- below the limits of raw and embedded postings and keeps others untouched.

DROP TABLE IF EXISTS t_text_merged;
DROP TABLE IF EXISTS t_text_rebuilt;

CREATE TABLE t_text_merged
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 128) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_phrase_search = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
    merge_text_indexes_and_projections_on_delete_only_merges = 1;

CREATE TABLE t_text_rebuilt
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 128) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_phrase_search = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
    merge_text_indexes_and_projections_on_delete_only_merges = 0;

-- Same data in both tables. The second insert replaces every third row of the first one, the third insert replaces
-- rows of the second one. Tokens: 'all' in every row, 'gone' only in replaced rows, 'few' in many rows of which
-- only a few survive, 'thin' in rows of which a few dozen survive, 'pair<N>' in three rows of which the first is replaced.
INSERT INTO t_text_merged SELECT number, 1,
    concat('all row', toString(number), ' pair', toString(intDiv(number, 3)), if(number % 3 = 0, ' gone', ''), if(number % 3 != 0 AND number % 1000 = 1, ' few', if(number % 3 = 0, ' few', '')),
        if(number % 3 = 0 OR number % 300 = 2, ' thin', ''), ' all tail') FROM numbers(30000);
INSERT INTO t_text_merged SELECT number, 2, concat('all fresh row', toString(number), ' fresh all') FROM numbers(0, 30000, 3);
INSERT INTO t_text_merged SELECT number, 3, concat('all newest ', toString(number % 7)) FROM numbers(0, 30000, 9);

INSERT INTO t_text_rebuilt SELECT number, 1,
    concat('all row', toString(number), ' pair', toString(intDiv(number, 3)), if(number % 3 = 0, ' gone', ''), if(number % 3 != 0 AND number % 1000 = 1, ' few', if(number % 3 = 0, ' few', '')),
        if(number % 3 = 0 OR number % 300 = 2, ' thin', ''), ' all tail') FROM numbers(30000);
INSERT INTO t_text_rebuilt SELECT number, 2, concat('all fresh row', toString(number), ' fresh all') FROM numbers(0, 30000, 3);
INSERT INTO t_text_rebuilt SELECT number, 3, concat('all newest ', toString(number % 7)) FROM numbers(0, 30000, 9);

OPTIMIZE TABLE t_text_merged FINAL;
OPTIMIZE TABLE t_text_rebuilt FINAL;

SELECT 'rows', (SELECT count() FROM t_text_merged), (SELECT count() FROM t_text_rebuilt);

SELECT 'tokens and formats';
SELECT
    (SELECT groupArray((token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings))
     FROM (SELECT * FROM mergeTreeTextIndex(currentDatabase(), t_text_merged, idx_text) ORDER BY token))
    =
    (SELECT groupArray((token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings))
     FROM (SELECT * FROM mergeTreeTextIndex(currentDatabase(), t_text_rebuilt, idx_text) ORDER BY token));

SELECT 'row ids and positions';
SELECT
    (SELECT groupArray((token, row_id, positions))
     FROM (SELECT token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), t_text_merged, idx_text) ORDER BY token, row_id))
    =
    (SELECT groupArray((token, row_id, positions))
     FROM (SELECT token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), t_text_rebuilt, idx_text) ORDER BY token, row_id));

SELECT 'dropped tokens';
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_text_merged, idx_text) WHERE token = 'gone';

SELECT 'cardinalities match the data';
SELECT token, i.cardinality, i.cardinality = d.rows
FROM mergeTreeTextIndex(currentDatabase(), t_text_merged, idx_text) AS i
INNER JOIN
(
    SELECT arrayJoin(arrayDistinct(splitByNonAlpha(text))) AS token, count() AS rows
    FROM t_text_merged GROUP BY token
) AS d USING (token)
WHERE token IN ('all', 'few', 'thin', 'fresh', 'newest', 'tail', 'row1', 'row2')
ORDER BY token;

SELECT 'phrase search';
SELECT count() FROM t_text_merged WHERE hasPhrase(text, 'fresh all') SETTINGS force_data_skipping_indices = 'idx_text';
SELECT count() FROM t_text_merged WHERE hasPhrase(text, 'all tail') SETTINGS force_data_skipping_indices = 'idx_text';

SYSTEM FLUSH LOGS part_log;

SELECT 'merged, not rebuilt';
SELECT table, ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_text_merged', 't_text_rebuilt') AND event_type = 'MergeParts' AND error = 0
ORDER BY table;

DROP TABLE t_text_merged;
DROP TABLE t_text_rebuilt;

-- Lightweight deletes in a plain MergeTree merged with the vertical algorithm, which filters deleted rows in the merging algorithm.
DROP TABLE IF EXISTS t_text_lwd_merged;
DROP TABLE IF EXISTS t_text_lwd_rebuilt;

CREATE TABLE t_text_lwd_merged
(
    id UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 128) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_phrase_search = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    merge_text_indexes_and_projections_on_delete_only_merges = 1;

CREATE TABLE t_text_lwd_rebuilt
(
    id UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 128) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_phrase_search = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    merge_text_indexes_and_projections_on_delete_only_merges = 0;

INSERT INTO t_text_lwd_merged SELECT number * 2, concat('even ', toString(number % 50), if(number % 5 = 0, ' doomed', ''), ' even') FROM numbers(20000);
INSERT INTO t_text_lwd_merged SELECT number * 2 + 1, concat('odd ', toString(number % 50), if(number % 7 = 0, ' doomed', ''), ' odd') FROM numbers(20000);
INSERT INTO t_text_lwd_rebuilt SELECT number * 2, concat('even ', toString(number % 50), if(number % 5 = 0, ' doomed', ''), ' even') FROM numbers(20000);
INSERT INTO t_text_lwd_rebuilt SELECT number * 2 + 1, concat('odd ', toString(number % 50), if(number % 7 = 0, ' doomed', ''), ' odd') FROM numbers(20000);

DELETE FROM t_text_lwd_merged WHERE hasToken(text, 'doomed') OR id % 1000 < 3;
DELETE FROM t_text_lwd_rebuilt WHERE hasToken(text, 'doomed') OR id % 1000 < 3;

OPTIMIZE TABLE t_text_lwd_merged FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_text_lwd_rebuilt FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'lightweight deletes';
SELECT (SELECT count() FROM t_text_lwd_merged), (SELECT count() FROM t_text_lwd_rebuilt);
SELECT
    (SELECT groupArray((token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions))
     FROM (SELECT token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions
           FROM mergeTreeTextIndex(currentDatabase(), t_text_lwd_merged, idx_text) ORDER BY token, row_id))
    =
    (SELECT groupArray((token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions))
     FROM (SELECT token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions
           FROM mergeTreeTextIndex(currentDatabase(), t_text_lwd_rebuilt, idx_text) ORDER BY token, row_id));
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_text_lwd_merged, idx_text) WHERE token = 'doomed';

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm, ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_text_lwd_merged', 't_text_lwd_rebuilt') AND event_type = 'MergeParts' AND error = 0
ORDER BY table;

DROP TABLE t_text_lwd_merged;
DROP TABLE t_text_lwd_rebuilt;

-- A merge that keeps at most a tenth of the source rows builds the index of the resulting part from them.

DROP TABLE IF EXISTS t_text_built;
DROP TABLE IF EXISTS t_text_built_rebuilt;

CREATE TABLE t_text_built
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 128) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_phrase_search = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
    merge_text_indexes_and_projections_on_delete_only_merges = 1;

CREATE TABLE t_text_built_rebuilt
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 128) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS index_granularity = 256, allow_experimental_text_index_phrase_search = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
    merge_text_indexes_and_projections_on_delete_only_merges = 0;

INSERT INTO t_text_built SELECT number % 1000, intDiv(number, 1000), concat('row ', toString(number % 1000), ' version ', toString(intDiv(number, 1000)))
FROM numbers(12000) SETTINGS max_block_size = 1000, max_insert_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0;
INSERT INTO t_text_built_rebuilt SELECT number % 1000, intDiv(number, 1000), concat('row ', toString(number % 1000), ' version ', toString(intDiv(number, 1000)))
FROM numbers(12000) SETTINGS max_block_size = 1000, max_insert_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0;

OPTIMIZE TABLE t_text_built FINAL;
OPTIMIZE TABLE t_text_built_rebuilt FINAL;

SELECT 'built';
SELECT (SELECT count() FROM t_text_built WHERE hasPhrase(text, 'version 11') SETTINGS force_data_skipping_indices = 'idx_text'),
    (SELECT count() FROM t_text_built WHERE hasToken(text, '10') SETTINGS force_data_skipping_indices = 'idx_text');
SELECT
    (SELECT groupArray((token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions))
     FROM (SELECT token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions
           FROM mergeTreeTextIndex(currentDatabase(), t_text_built, idx_text) ORDER BY token, row_id))
    =
    (SELECT groupArray((token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions))
     FROM (SELECT token, cardinality, num_posting_blocks, has_embedded_postings, has_raw_postings, has_compressed_postings, row_id, positions
           FROM mergeTreeTextIndex(currentDatabase(), t_text_built_rebuilt, idx_text) ORDER BY token, row_id));

SYSTEM FLUSH LOGS part_log;
SELECT table, ProfileEvents['MergedTextIndexes'], ProfileEvents['RebuiltTextIndexes'] FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_text_built', 't_text_built_rebuilt') AND event_type = 'MergeParts' AND error = 0
ORDER BY table;

DROP TABLE t_text_built;
DROP TABLE t_text_built_rebuilt;
