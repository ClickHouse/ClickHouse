-- `MATERIALIZE INDEX` on a packed or compact part must build text indexes in flushed segments.

DROP TABLE IF EXISTS tab_packed;
DROP TABLE IF EXISTS tab_compact;

CREATE TABLE tab_packed (
    id UInt64,
    s String,
    s2 String,
    INDEX idx_existing s2 TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_level_for_full_part_storage = 2, min_bytes_for_wide_part = 0;

CREATE TABLE tab_compact (
    id UInt64,
    s String,
    s2 String,
    INDEX idx_existing s2 TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_full_part_storage = 0, min_bytes_for_wide_part = '1G';

INSERT INTO tab_packed SELECT number, concat('token', toString(number)), concat('other', toString(number)) FROM numbers(20000);
INSERT INTO tab_compact SELECT number, concat('token', toString(number)), concat('other', toString(number)) FROM numbers(20000);

SELECT table, part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('tab_packed', 'tab_compact') AND active
ORDER BY table;

ALTER TABLE tab_packed ADD INDEX idx s TYPE text(tokenizer = 'array');
ALTER TABLE tab_compact ADD INDEX idx s TYPE text(tokenizer = 'array');

ALTER TABLE tab_packed MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
ALTER TABLE tab_compact MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS part_log;

SELECT table, ProfileEvents['TextIndexTemporarySegmentsWritten'] > 1
FROM system.part_log
WHERE event_date >= yesterday()
    AND database = currentDatabase()
    AND table IN ('tab_packed', 'tab_compact')
    AND event_type = 'MutatePart'
    AND error = 0
ORDER BY table;

DETACH TABLE tab_packed;
ATTACH TABLE tab_packed;
DETACH TABLE tab_compact;
ATTACH TABLE tab_compact;

CHECK TABLE tab_packed SETTINGS check_query_single_value_result = 1;
CHECK TABLE tab_compact SETTINGS check_query_single_value_result = 1;

SELECT count() FROM tab_packed WHERE s = 'token12345' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_packed WHERE s2 = 'other12345' SETTINGS force_data_skipping_indices = 'idx_existing';
SELECT count() FROM tab_compact WHERE s = 'token12345' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM tab_compact WHERE s2 = 'other12345' SETTINGS force_data_skipping_indices = 'idx_existing';

DROP TABLE tab_packed;
DROP TABLE tab_compact;
