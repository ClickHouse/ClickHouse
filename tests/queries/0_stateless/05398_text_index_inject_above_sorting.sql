-- Tags: no-parallel-replicas
-- With parallel replicas, the steps above the subquery are not rewritten.

-- Text-search functions above a sorting step (in the SELECT list, or over a subquery) apply the index
-- preprocessor and tokenizer, and the rewrite keeps the step's output columns.

SET enable_analyzer = 1;
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

SELECT '-- the ORDER BY column is not added to the result';
SELECT hasToken(s, 'foo') FROM tab ORDER BY id;
SELECT hasAllTokens(s, 'foo bar') FROM tab ORDER BY id;
SELECT * FROM (SELECT hasAnyTokens(s, 'foo') FROM tab ORDER BY id);

CREATE TABLE dst (h UInt8) ENGINE = Memory;
INSERT INTO dst SELECT hasToken(s, 'foo') FROM tab ORDER BY id;
SELECT * FROM dst;

SELECT x FROM (SELECT hasToken(s, 'foo') AS x, id FROM tab ORDER BY id LIMIT 10) WHERE id > 0 ORDER BY id;

SELECT '-- a filter below the sorting step';
SELECT hasToken(s, 'foo') FROM tab WHERE hasToken(s, 'bar') ORDER BY id;
SELECT hasToken(s, 'foo'), id FROM tab QUALIFY hasToken(s, 'bar') ORDER BY id;

SELECT '-- the outer ORDER BY does not include the function result';
SELECT id, hasToken(x, 'foo') AS h FROM (SELECT id, s AS x FROM tab ORDER BY id LIMIT 10) ORDER BY id, h;
SELECT id, hasToken(x, 'foo') AS h FROM (SELECT id, s AS x FROM tab ORDER BY id LIMIT 10) ORDER BY id;

SELECT '-- the subquery keeps the name of the indexed column';
SELECT countIf(hasToken(s, 'foo')) FROM tab;
SELECT countIf(hasToken(x, 'foo')) FROM (SELECT s AS x FROM tab ORDER BY id LIMIT 10);
SELECT countIf(hasToken(s, 'foo')) FROM (SELECT s FROM tab ORDER BY id LIMIT 10);
SELECT id, hasToken(s, 'foo') FROM (SELECT id, s FROM tab ORDER BY id LIMIT 10) ORDER BY id;

DROP TABLE dst;
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

SELECT hasAnyTokens(s, 'a b') FROM tab ORDER BY id;
SELECT countIf(hasAnyTokens(s, 'a b')) FROM (SELECT s FROM tab ORDER BY id LIMIT 10);
SELECT count() FROM (SELECT s FROM tab ORDER BY id LIMIT 10) WHERE hasAnyTokens(s, 'a b');

DROP TABLE tab;
