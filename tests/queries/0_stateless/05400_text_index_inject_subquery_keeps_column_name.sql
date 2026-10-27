-- Tags: no-parallel-replicas
-- With parallel replicas, the steps above the subquery are not rewritten.

-- A text-search function over a subquery that keeps the name of the indexed column applies the index
-- preprocessor and tokenizer, the same as with an alias or in WHERE.
-- https://github.com/ClickHouse/ClickHouse/issues/125854

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

SELECT '-- preprocessor';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'FOO bar'), (2, 'foo bar'), (3, 'zzz');

SELECT countIf(hasToken(s, 'foo')) FROM tab;
SELECT countIf(hasToken(x, 'foo')) FROM (SELECT s AS x FROM tab ORDER BY id LIMIT 10);
SELECT countIf(hasToken(s, 'foo')) FROM (SELECT s FROM tab ORDER BY id LIMIT 10);
SELECT count() FROM (SELECT s FROM tab ORDER BY id LIMIT 10) WHERE hasToken(s, 'foo');
SELECT id, hasToken(s, 'foo') FROM (SELECT id, s FROM tab ORDER BY id LIMIT 10) ORDER BY id;

DROP TABLE tab;

SELECT '-- tokenizer';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByString(['|']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'a b|c'), (2, 'a b'), (3, 'a|b');

SELECT countIf(hasAnyTokens(s, 'a b')) FROM tab;
SELECT countIf(hasAnyTokens(s, 'a b')) FROM (SELECT s FROM tab ORDER BY id LIMIT 10);
SELECT count() FROM (SELECT s FROM tab ORDER BY id LIMIT 10) WHERE hasAnyTokens(s, 'a b');

DROP TABLE tab;
