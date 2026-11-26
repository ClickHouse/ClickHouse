-- Tags: no-parallel-replicas
-- LEFT ARRAY JOIN uses a text index on the array unless the filter accepts the element that it adds for an empty array

SET use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_text_left_aj;
CREATE TABLE t_text_left_aj (id UInt64, ids Array(String), INDEX idx ids TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- 10 granules, the first row of each granule has an empty array
INSERT INTO t_text_left_aj SELECT number, if(number % 4 = 0, [], [toString(number), toString(number + 100)]) FROM numbers(40);

SELECT '-- equals';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x = '7') WHERE explain ILIKE '%Granules: 1/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj WHERE arrayJoin(emptyArrayToSingle(ids)) = '7') WHERE explain ILIKE '%Granules: 1/10%';
SELECT groupArray(x) FROM (SELECT x FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x = '7') SETTINGS force_data_skipping_indices = 'idx';
SELECT groupArray(x) FROM (SELECT arrayJoin(emptyArrayToSingle(ids)) AS x FROM t_text_left_aj WHERE x = '7') SETTINGS query_plan_lower_array_join_function = 0;

SELECT '-- in';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN ('7', '130')) WHERE explain ILIKE '%Granules: 2/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN (SELECT arrayJoin(['7', '130']))) WHERE explain ILIKE '%Granules: 2/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x GLOBAL IN (SELECT arrayJoin(['7', '130']))) WHERE explain ILIKE '%Granules: 2/10%';
SELECT groupArray(x) FROM (SELECT x FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN ('7', '130') ORDER BY x);

SELECT '-- transform_null_in';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN (SELECT arrayJoin(['7', '130'])) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Granules: 2/10%';
SELECT groupArray(x) FROM (SELECT x FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN (SELECT arrayJoin(['7', '130'])) ORDER BY x) SETTINGS transform_null_in = 1;

SELECT '-- the filter accepts the empty string of an empty array: the index is not used';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x = '' SETTINGS optimize_empty_string_comparisons = 0) WHERE explain ILIKE '%Name: idx%';
SELECT count() FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x = '' SETTINGS optimize_empty_string_comparisons = 0;
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN ('7', '')) WHERE explain ILIKE '%Name: idx%';
SELECT count() FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN ('7', '');
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj LEFT ARRAY JOIN ids AS x WHERE x IN (SELECT arrayJoin([NULL, '7'])) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Name: idx%';

DROP TABLE t_text_left_aj;

SELECT '-- FixedString: the index has the zero bytes of an element, but not of an empty array';
DROP TABLE IF EXISTS t_text_left_aj_fs;
CREATE TABLE t_text_left_aj_fs (id UInt64, ids Array(FixedString(3)), INDEX idx ids TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_text_left_aj_fs SELECT number, if(number % 4 = 0, [], [toFixedString(toString(number), 3)]) FROM numbers(40);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj_fs LEFT ARRAY JOIN ids AS x WHERE x = '7') WHERE explain ILIKE '%Granules: 1/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj_fs LEFT ARRAY JOIN ids AS x WHERE x = toFixedString('', 3)) WHERE explain ILIKE '%Name: idx%';
SELECT count() FROM t_text_left_aj_fs LEFT ARRAY JOIN ids AS x WHERE x = toFixedString('', 3);
SELECT count() FROM t_text_left_aj_fs LEFT ARRAY JOIN ids AS x WHERE x = toFixedString('', 3) SETTINGS use_skip_indexes_on_data_read = 1;
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_left_aj_fs LEFT ARRAY JOIN ids AS x WHERE x IN ('7', '')) WHERE explain ILIKE '%Name: idx%';
SELECT count() FROM t_text_left_aj_fs LEFT ARRAY JOIN ids AS x WHERE x IN ('7', '');
DROP TABLE t_text_left_aj_fs;

SELECT '-- an index on the padded array';
DROP TABLE IF EXISTS t_text_padded_aj;
CREATE TABLE t_text_padded_aj (id UInt64, ids Array(FixedString(3)), INDEX idx emptyArrayToSingle(ids) TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
-- only the first granule has an empty array
INSERT INTO t_text_padded_aj SELECT number, if(number = 0, [], [toFixedString(toString(number), 3)]) FROM numbers(40);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_padded_aj LEFT ARRAY JOIN ids AS x WHERE x = toFixedString('', 3)) WHERE explain ILIKE '%Granules: 1/10%';
SELECT count() FROM t_text_padded_aj LEFT ARRAY JOIN ids AS x WHERE x = toFixedString('', 3);
DROP TABLE t_text_padded_aj;
