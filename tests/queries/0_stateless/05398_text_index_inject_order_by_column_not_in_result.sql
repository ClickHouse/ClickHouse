-- Rewriting a text-search function in the SELECT list above a sorting step does not add the ORDER BY column to the result.
-- https://github.com/ClickHouse/ClickHouse/issues/125836

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;
SET query_plan_execute_functions_after_sorting = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dst;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'FOO bar'), (2, 'foo bar'), (3, 'zzz');

SELECT hasToken(s, 'foo') FROM tab ORDER BY id;
SELECT hasAllTokens(s, 'foo bar') FROM tab ORDER BY id;
SELECT * FROM (SELECT hasAnyTokens(s, 'foo') FROM tab ORDER BY id);
SELECT x FROM (SELECT hasToken(s, 'foo') AS x, id FROM tab ORDER BY id LIMIT 10) WHERE id > 0 ORDER BY id;

CREATE TABLE dst (h UInt8) ENGINE = Memory;
INSERT INTO dst SELECT hasToken(s, 'foo') FROM tab ORDER BY id;
SELECT * FROM dst;

DROP TABLE dst;
DROP TABLE tab;
