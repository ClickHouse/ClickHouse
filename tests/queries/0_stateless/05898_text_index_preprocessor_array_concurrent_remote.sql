-- Concurrent shard queries build the text index of one table; the preprocessor of an Array index must not rewrite the table's index expression.
SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    INDEX idx arrayMap(x -> concat(x, 'x'), splitByChar(' ', s))
        TYPE text(tokenizer = 'array', preprocessor = lower(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s))))
)
ENGINE = MergeTree ORDER BY s;

INSERT INTO tab VALUES ('Hello World'), ('FOO bar'), ('');

SELECT count() FROM remote('127.0.0.{1,2,3,4}', currentDatabase(), tab)
WHERE hasAnyTokens(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ['hellox'])
SETTINGS prefer_localhost_replica = 0;

DROP TABLE tab;
