-- Tags: no-parallel-replicas
-- With parallel replicas, the steps above the subquery are not rewritten.

-- A text-search function above a sorting step reads the indexed column, not another column named like the index preprocessor or postprocessor expression.
-- https://github.com/ClickHouse/ClickHouse/issues/127013

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET query_plan_execute_functions_after_sorting = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s String,
    t String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'foo bar', 'zzz'), (2, 'zzz', 'foo');

SELECT s, hasToken(s, 'foo') AS h, t AS `lower(s)` FROM tab ORDER BY s;
SELECT x, hasToken(x, 'foo') AS h, lower(s) FROM (SELECT id, x, t AS s FROM (SELECT id, s AS x, t FROM tab ORDER BY id LIMIT 10) ORDER BY id LIMIT 10) ORDER BY s, x;
SELECT s, hasAnyTokens(s, 'foo') AS h, t AS `lower(s)` FROM tab ORDER BY s;
SELECT s, hasAllTokens(s, 'foo bar') AS h, t AS `lower(s)` FROM tab ORDER BY s;

DROP TABLE tab;

DROP TABLE IF EXISTS tab_post;

CREATE TABLE tab_post
(
    id UInt64,
    s String,
    t String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_post VALUES (1, 'FOO bar', 'zzz'), (2, 'zzz', 'foo');

SELECT s, hasToken(s, 'foo') AS h, t AS `lower(s)` FROM tab_post ORDER BY s;

DROP TABLE tab_post;
