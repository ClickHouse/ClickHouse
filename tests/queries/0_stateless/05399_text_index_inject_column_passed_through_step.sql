-- Tags: no-parallel-replicas
-- With parallel replicas, the steps above the subquery are not rewritten.

-- A text-search function above a sorting step applies the index preprocessor when a step below passes the indexed
-- column through without taking it as an input, e.g. an outer ORDER BY without the function result.
-- https://github.com/ClickHouse/ClickHouse/issues/125855

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;
SET query_plan_execute_functions_after_sorting = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'FOO bar'), (2, 'foo bar'), (3, 'zzz');

SELECT '-- the outer ORDER BY does not include the function result';
SELECT id, hasToken(x, 'foo') AS h FROM (SELECT id, s AS x FROM tab ORDER BY id LIMIT 10) ORDER BY id, h;
SELECT id, hasToken(x, 'foo') AS h FROM (SELECT id, s AS x FROM tab ORDER BY id LIMIT 10) ORDER BY id;

SELECT '-- a filter below the sorting step';
SELECT hasToken(s, 'foo') FROM tab WHERE hasToken(s, 'bar') ORDER BY id;
SELECT hasToken(s, 'foo'), id FROM tab QUALIFY hasToken(s, 'bar') ORDER BY id;

DROP TABLE tab;
