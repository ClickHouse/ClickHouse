-- Tags: no-parallel-replicas
-- a text index on an array is used for arrayJoin(arr) = const and arrayJoin(arr) IN (set), like for has and hasAny

SET use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_text_aj;
CREATE TABLE t_text_aj (id UInt64, ids Array(String), INDEX idx ids TYPE text(tokenizer = 'array') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- 10 granules, each value is in one row, twice for the values below 100
INSERT INTO t_text_aj SELECT number, [toString(number), toString(number + 100), toString(number)] FROM numbers(40);

SELECT '-- equals';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) = '7') WHERE explain ILIKE '%Granules: 1/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj ARRAY JOIN ids AS x WHERE x = '7') WHERE explain ILIKE '%Granules: 1/10%';
SELECT groupArray(x) FROM (SELECT arrayJoin(ids) AS x FROM t_text_aj WHERE x = '7');

SELECT '-- in';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) IN ('7', '130')) WHERE explain ILIKE '%Granules: 2/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) IN (SELECT arrayJoin(['7', '130']))) WHERE explain ILIKE '%Granules: 2/10%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) GLOBAL IN (SELECT arrayJoin(['7', '130']))) WHERE explain ILIKE '%Granules: 2/10%';
SELECT groupArray(x) FROM (SELECT arrayJoin(ids) AS x FROM t_text_aj WHERE x IN ('7', '130') ORDER BY x);

SELECT '-- transform_null_in';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) IN (SELECT arrayJoin(['7', '130'])) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Granules: 2/10%';
SELECT groupArray(x) FROM (SELECT arrayJoin(ids) AS x FROM t_text_aj WHERE x IN (SELECT arrayJoin(['7', '130'])) ORDER BY x) SETTINGS transform_null_in = 1;

SELECT '-- the index is not used';
-- notEquals and NOT IN keep the other elements of a matching row
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) != '7') WHERE explain ILIKE '%Name: idx%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) NOT IN ('7')) WHERE explain ILIKE '%Name: idx%';
-- a NULL in the set matches with transform_null_in = 1, and the index has no NULL
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj WHERE arrayJoin(ids) IN (SELECT arrayJoin([NULL, '7'])) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Name: idx%';
-- LEFT ARRAY JOIN adds an empty string for an empty array, and the index has no empty strings
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_text_aj LEFT ARRAY JOIN ids AS x WHERE x = '7') WHERE explain ILIKE '%Name: idx%';

SELECT '-- the index does not replace the element filter by a row filter';
SELECT groupArray(x) FROM (SELECT arrayJoin(ids) AS x FROM t_text_aj WHERE x = '7') SETTINGS query_plan_lower_array_join_function = 0, query_plan_direct_read_from_text_index = 1;
SELECT groupArray(x) FROM (SELECT arrayJoin(ids) AS x FROM t_text_aj WHERE x IN ('7', '130') ORDER BY x) SETTINGS query_plan_lower_array_join_function = 0, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM t_text_aj WHERE arrayJoin(ids) = '7' SETTINGS query_plan_lower_array_join_function = 0, query_plan_direct_read_from_text_index = 1;
SELECT count() FROM t_text_aj WHERE arrayJoin(ids) = '7' SETTINGS query_plan_lower_array_join_function = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_aj;
