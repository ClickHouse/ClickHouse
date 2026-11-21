-- Tags: no-parallel-replicas

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS test_text_index_case_insensitive;

CREATE TABLE test_text_index_case_insensitive
(
    id UInt64,
    message String,
    INDEX idx_message message TYPE text(tokenizer = ngrams(4)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_text_index_case_insensitive VALUES
    (1, 'audit DENIED user=1'),
    (2, 'audit denied user=2'),
    (3, 'login Refused user=3'),
    (4, 'request ok id=4');

SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i)(denied|refused)');
SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i:denied|refused)');
SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i)(DENIED|REFUSED)');
SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i)(Denied|Refused)');
SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i)denied');
SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i)(denied)');
SELECT count() FROM test_text_index_case_insensitive WHERE match(message, '(?i)denied|refused');

DROP TABLE test_text_index_case_insensitive;
