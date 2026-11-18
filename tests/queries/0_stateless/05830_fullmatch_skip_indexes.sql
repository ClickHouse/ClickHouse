-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the EXPLAIN output with the granule counts differs with parallel replicas
-- Tests that fullMatch utilizes the text index and the token bloom filter index
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab_text;
DROP TABLE IF EXISTS tab_tokenbf;

CREATE TABLE tab_text
(
    id UInt32,
    str String,
    INDEX idx(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

CREATE TABLE tab_tokenbf
(
    id UInt32,
    str String,
    INDEX idx str TYPE tokenbf_v1(256, 2, 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_text VALUES (1, 'Well, Hello ClickHouse !'), (2, 'Well, Hello World !'), (3, 'Good Weather !'), (4, 'Say Hello !'), (5, 'Its An OLAP Database'), (6, 'True World Champion');
INSERT INTO tab_tokenbf VALUES (1, 'Well, Hello ClickHouse !'), (2, 'Well, Hello World !'), (3, 'Good Weather !'), (4, 'Say Hello !'), (5, 'Its An OLAP Database'), (6, 'True World Champion');

-- The implicit anchors do not hide the literal parts of the pattern from the index analysis.
-- Read 2/6 granules: alternatives ' Hello ClickHouse ', ' Hello World '

SELECT * FROM tab_text WHERE fullMatch(str, '.* Hello (ClickHouse|World) .*') ORDER BY id;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab_text WHERE fullMatch(str, '.* Hello (ClickHouse|World) .*') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';

SELECT '---';

SELECT * FROM tab_tokenbf WHERE fullMatch(str, '.* Hello (ClickHouse|World) .*') ORDER BY id;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab_tokenbf WHERE fullMatch(str, '.* Hello (ClickHouse|World) .*') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';

SELECT '---';

-- A plain literal pattern must match the whole value.
-- Read 1/6 granules: required string 'Its An OLAP Database'

SELECT * FROM tab_text WHERE fullMatch(str, 'Its An OLAP Database') ORDER BY id;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab_text WHERE fullMatch(str, 'Its An OLAP Database') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';

SELECT '---';

SELECT * FROM tab_tokenbf WHERE fullMatch(str, 'Its An OLAP Database') ORDER BY id;
SELECT trim(explain)
FROM
(
    EXPLAIN PLAN indexes = 1
    SELECT * FROM tab_tokenbf WHERE fullMatch(str, 'Its An OLAP Database') ORDER BY id
)
WHERE
    explain LIKE '%Granules: %';

DROP TABLE tab_text;
DROP TABLE tab_tokenbf;
