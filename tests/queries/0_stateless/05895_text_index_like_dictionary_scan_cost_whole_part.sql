-- Tags: no-parallel-replicas
-- The cost check of the text index pattern dictionary scan also applies when the primary key keeps the whole
-- part. Every row carries its own short token, so matching the tokens of the dictionary costs more than
-- filtering the rows of the column, and the scan is skipped. The result is the same as without the scan.

SET use_query_condition_cache = 0;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 8192)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO tab SELECT number, concat('row', toString(number), if(number % 10000 = 0, ' needle', '')) FROM numbers(200000);
OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE message LIKE '%needle%' SETTINGS log_comment = 'like';
SELECT count() FROM tab WHERE position(message, 'needle') > 0 SETTINGS log_comment = 'position', query_plan_direct_read_from_text_index = 1;
SELECT count() FROM tab WHERE message LIKE '%needle%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['TextIndexPatternScanSkippedByCost'] > 0 AS scan_skipped
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('like', 'position')
ORDER BY log_comment;

DROP TABLE tab;
