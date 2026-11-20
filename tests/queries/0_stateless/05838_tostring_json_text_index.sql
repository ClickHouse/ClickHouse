-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database, no-random-merge-tree-settings
-- A text index over JSONAllValues(json) must also serve filters that wrap the JSON subcolumn
-- into toString(): toString() renders the value's text representation, which is exactly what
-- JSONAllValues stores (string values as-is, other types as text), same as an explicit CAST.
SET enable_json_type = 1;

DROP TABLE IF EXISTS test_tostring_json_text_index;

CREATE TABLE test_tostring_json_text_index
(
    id UInt32,
    data JSON,
    INDEX vals JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO test_tostring_json_text_index (id, data) VALUES
    (1, '{"host": "host_aaa", "level": "info"}'),
    (2, '{"host": "host_bbb", "level": "info"}'),
    (3, '{"host": "host_ccc", "level": "warn"}'),
    (4, '{"host": "host_aaa", "level": "warn"}'),
    (5, '{"host": "other_aaa", "level": "info"}'),
    (6, '{"host": "other_bbb", "level": "warn"}'),
    (7, '{"n": 42}'),
    (8, '{}');

SELECT 'results: plain subcolumn';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE data.host = 'host_aaa';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE data.host LIKE '%bbb%';

SELECT 'results: toString-wrapped (index path)';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE toString(data.host) = 'host_aaa';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE toString(data.host) LIKE '%bbb%';

SELECT 'results: toString-wrapped (brute force, no skip indexes)';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE toString(data.host) = 'host_aaa' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE toString(data.host) LIKE '%bbb%' SETTINGS use_skip_indexes = 0;

SELECT 'index condition: plain subcolumn, absent value';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index WHERE data.host = 'absent'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

SELECT 'results: toString-wrapped non-string value';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE toString(data.n) = '42';
SELECT groupArray(id) FROM test_tostring_json_text_index WHERE toString(data.n) = '42' SETTINGS use_skip_indexes = 0;

SELECT 'index condition: toString-wrapped, absent value';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index WHERE toString(data.host) = 'absent'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

SELECT 'index condition: toString-wrapped, LIKE';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index WHERE toString(data.host) LIKE '%zzz%'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

SELECT 'index condition: toString-wrapped, absent non-string value';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index WHERE toString(data.n) = '999'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

DROP TABLE test_tostring_json_text_index;
