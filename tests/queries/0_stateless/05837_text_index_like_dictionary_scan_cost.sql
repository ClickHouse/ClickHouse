-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- The LIKE dictionary scan of a text index reads every dictionary block of a part, however few rows the
-- primary key keeps. It is skipped for a part when those rows take fewer bytes in the indexed column than
-- the dictionary blocks to scan, and kept when the readable rows are larger.

SET use_query_condition_cache = 0;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    ts DateTime,
    svc LowCardinality(String),
    body String,
    INDEX idx_body body TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY (svc, ts)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1024, index_granularity_bytes = '10Mi';

-- One unique token per row makes a large dictionary. The rest of the body is punctuation: it adds bytes
-- to the column but no tokens to the dictionary.
INSERT INTO tab
SELECT
    toDateTime('2026-01-01 00:00:00') + intDiv(number, 4),
    ['api', 'web', 'worker', 'db'][number % 4 + 1],
    concat(
        'request', lower(hex(cityHash64(number))), ' ',
        translate(randomPrintableASCII(300), 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', '!#$%&()*+,-./:;<=>?@[]^_{|}~!#$%&()*+,-./:;<=>?@[]^_{|}~!#$%&('),
        if(intDiv(number, 4) % 50 = 0, ' OOMKilled', ''))
FROM numbers(200000);

OPTIMIZE TABLE tab FINAL;

-- A narrow range: the scan is skipped, the result is the same as without the scan.
SELECT count() FROM tab
WHERE svc = 'api' AND ts >= toDateTime('2026-01-01 01:00:00') AND ts < toDateTime('2026-01-01 01:02:00') AND body LIKE '%OOMKilled%'
SETTINGS log_comment = 'narrow scan on';

SELECT count() FROM tab
WHERE svc = 'api' AND ts >= toDateTime('2026-01-01 01:00:00') AND ts < toDateTime('2026-01-01 01:02:00') AND body LIKE '%OOMKilled%'
SETTINGS log_comment = 'narrow scan off', use_text_index_like_evaluation_by_dictionary_scan = 0;

-- A wide range: the readable rows take more bytes than the dictionary, so the scan is kept.
SELECT count() FROM tab
WHERE svc = 'api' AND body LIKE '%OOMKilled%'
SETTINGS log_comment = 'wide scan on';

SELECT count() FROM tab
WHERE svc = 'api' AND body LIKE '%OOMKilled%'
SETTINGS log_comment = 'wide scan off', use_text_index_like_evaluation_by_dictionary_scan = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexPatternScanSkippedByCost'] > 0 AS scan_skipped,
    ProfileEvents['TextIndexReadDictionaryBlocks'] > 0 AS dictionary_read
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('narrow scan on', 'wide scan on')
ORDER BY log_comment;

DROP TABLE tab;
