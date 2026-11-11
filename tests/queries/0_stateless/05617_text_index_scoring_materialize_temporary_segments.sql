-- `MATERIALIZE INDEX` of a text index with `scoring = 'bm25'` builds the index in several temporary segments.
-- The row ids of every segment after the first continue from the rows of the previous segments,
-- while its document lengths start from its own first row: a compressed posting list in such a segment
-- must take the document lengths of its rows with the offset of the segment.

DROP TABLE IF EXISTS tab_scoring_segments;

CREATE TABLE tab_scoring_segments
(
    id UInt32,
    body_bitpacking String,
    body_pfor String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    index_granularity = 1000,
    index_granularity_bytes = '10Mi',
    text_index_max_processed_tokens_before_flush = 1,
    allow_experimental_text_index_scoring = 1;

-- `common`: every row, 1-3 times, a compressed posting list in every segment; `u<n>`: a token per row.
INSERT INTO tab_scoring_segments SELECT number, body, body FROM (SELECT number, concat(repeat('common ', number % 3 + 1), 'u', toString(number)) AS body FROM numbers(4000));
OPTIMIZE TABLE tab_scoring_segments FINAL;

ALTER TABLE tab_scoring_segments ADD INDEX idx_bitpacking(body_bitpacking) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25');
ALTER TABLE tab_scoring_segments ADD INDEX idx_pfor(body_pfor) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'pfor', scoring = 'bm25');

-- The mutation of a wide part reads a granule per block, and every block exceeds the limit of processed tokens: a segment per granule and index.
ALTER TABLE tab_scoring_segments MATERIALIZE INDEX idx_bitpacking, MATERIALIZE INDEX idx_pfor SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS part_log;

SELECT 'temporary segments', ProfileEvents['TextIndexTemporarySegmentsWritten']
FROM system.part_log
WHERE event_date >= yesterday()
    AND database = currentDatabase()
    AND table = 'tab_scoring_segments'
    AND event_type = 'MutatePart'
    AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '-- postings of `common`';
SELECT count(), sum(weight), min(row_id), max(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_segments, idx_bitpacking) WHERE token = 'common';
SELECT count(), sum(weight), min(row_id), max(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_segments, idx_pfor) WHERE token = 'common';

SELECT '-- all postings match the rows';
SELECT count() FROM
(
    SELECT row_id, weight FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_segments, idx_bitpacking) WHERE token = 'common'
    EXCEPT SELECT toUInt32(_part_offset), toUInt32(id % 3 + 1) FROM tab_scoring_segments
);
SELECT count() FROM
(
    SELECT row_id, weight FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_segments, idx_pfor) WHERE token = 'common'
    EXCEPT SELECT toUInt32(_part_offset), toUInt32(id % 3 + 1) FROM tab_scoring_segments
);
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_segments, idx_pfor) WHERE startsWith(token, 'u');

SELECT '-- filtering with the indexes';
SELECT count() FROM tab_scoring_segments WHERE hasToken(body_bitpacking, 'common') SETTINGS force_data_skipping_indices = 'idx_bitpacking';
SELECT count(), any(id) FROM tab_scoring_segments WHERE hasToken(body_pfor, 'u3210') SETTINGS force_data_skipping_indices = 'idx_pfor';

DROP TABLE tab_scoring_segments;
