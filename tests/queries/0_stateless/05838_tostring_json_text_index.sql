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

SELECT 'render settings: conversion-wrapped predicates must not use the index';

DROP TABLE IF EXISTS test_tostring_json_text_index_dt;
CREATE TABLE test_tostring_json_text_index_dt
(
    id UInt32,
    data JSON(ts DateTime64(6, 'UTC')),
    INDEX vals JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_tostring_json_text_index_dt (id, data) VALUES
    (1, '{"ts": "2024-01-01 00:00:00.123000"}'),
    (2, '{"ts": "2024-01-01 00:00:00.123456"}'),
    (3, '{"ts": "2024-01-01 00:00:05.000000"}');

SELECT 'results: DateTime64 path, default settings (index still used)';
SELECT groupArray(id) FROM test_tostring_json_text_index_dt WHERE toString(data.ts) = '2024-01-01 00:00:00.123000';
SELECT groupArray(id) FROM test_tostring_json_text_index_dt WHERE toString(data.ts) = '2024-01-01 00:00:00.123000' SETTINGS use_skip_indexes = 0;
SELECT 'index condition: toString-wrapped DateTime64, default settings (index used)';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index_dt WHERE toString(data.ts) = '2024-01-01 00:00:00.123000'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1;

SELECT 'results: DateTime64 path, cut_trailing_zeros (rendered text differs from indexed text)';
SELECT groupArray(id) FROM test_tostring_json_text_index_dt WHERE toString(data.ts) = '2024-01-01 00:00:00.123';
SELECT groupArray(id) FROM test_tostring_json_text_index_dt WHERE toString(data.ts) = '2024-01-01 00:00:00.123' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM test_tostring_json_text_index_dt WHERE CAST(data.ts, 'String') = '2024-01-01 00:00:00.123';
SELECT groupArray(id) FROM test_tostring_json_text_index_dt WHERE CAST(data.ts, 'String') = '2024-01-01 00:00:00.123' SETTINGS use_skip_indexes = 0;

SELECT 'index condition: toString-wrapped, cut_trailing_zeros (no pruning)';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index_dt WHERE toString(data.ts) = '2024-01-01 00:00:00.123'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

SELECT 'index condition: CAST-wrapped, cut_trailing_zeros (no pruning)';
SELECT groupArray(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tostring_json_text_index_dt WHERE CAST(data.ts, 'String') = '2024-01-01 00:00:00.123'
) WHERE explain LIKE '%vals%' OR explain LIKE '%Granules%' OR explain LIKE '%Condition%';

SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;

DROP TABLE test_tostring_json_text_index_dt;

DROP TABLE IF EXISTS test_tostring_json_text_index_bool;
CREATE TABLE test_tostring_json_text_index_bool
(
    id UInt32,
    data JSON(b Bool),
    INDEX vals JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO test_tostring_json_text_index_bool (id, data) VALUES (1, '{"b": true}');

SET bool_true_representation = 'YES';

SELECT 'results: Bool path, custom representation';
SELECT groupArray(id) FROM test_tostring_json_text_index_bool WHERE toString(data.b) = 'YES';
SELECT groupArray(id) FROM test_tostring_json_text_index_bool WHERE toString(data.b) = 'YES' SETTINGS use_skip_indexes = 0;

SET bool_true_representation = 'true';

DROP TABLE test_tostring_json_text_index_bool;
