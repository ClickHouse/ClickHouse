-- emptyArrayToSingle and LEFT ARRAY JOIN pad an empty array with the default of the element type.
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

SELECT emptyArrayToSingle(CAST([], 'Array(Enum8(\'x\' = 1, \'y\' = 2))'));
SELECT emptyArrayToSingle(materialize(CAST([], 'Array(Enum8(\'x\' = 1, \'y\' = 2))')));
SELECT emptyArrayToSingle(CAST(arrayWithConstant(number, 'y'), 'Array(Enum8(\'x\' = 1, \'y\' = 2))')) FROM numbers(3);
SELECT emptyArrayToSingle(materialize(CAST([], 'Array(Enum16(\'x\' = 1000, \'y\' = 2000))')));
SELECT emptyArrayToSingle(materialize(CAST([], 'Array(Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2))')));
SELECT emptyArrayToSingle(materialize(CAST([], 'Array(Tuple(e Enum8(\'x\' = 1, \'y\' = 2), n UInt8))')));
SELECT emptyArrayToSingle(materialize(CAST([], 'Array(JSON(a Enum8(\'x\' = 1, \'y\' = 2)))')));
SELECT emptyArrayToSingle(materialize(CAST([], 'Array(Nullable(Enum8(\'x\' = 1, \'y\' = 2)))'))), emptyArrayToSingle(materialize(CAST([], 'Array(UInt8)'))), emptyArrayToSingle(materialize(CAST([], 'Array(String)')));
SELECT length(emptyArrayToSingle([])), length(emptyArrayToSingle(materialize([])));
SELECT kv FROM (SELECT CAST(map(), 'Map(String, Enum8(\'x\' = 1, \'y\' = 2))') AS m) LEFT ARRAY JOIN m AS kv;

DROP TABLE IF EXISTS t_left_array_join_enum;
CREATE TABLE t_left_array_join_enum (id UInt32, arr Array(Enum8('x' = 1, 'y' = 2)), INDEX idx arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_left_array_join_enum SELECT number, if(number < 4, ['y'], []) FROM numbers(8);
SELECT id, e FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e ORDER BY id;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e = CAST('x', 'Enum8(\'x\' = 1, \'y\' = 2)')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e = CAST('x', 'Enum8(\'x\' = 1, \'y\' = 2)');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e IN (CAST('x', 'Enum8(\'x\' = 1, \'y\' = 2)'))) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e IN (CAST('x', 'Enum8(\'x\' = 1, \'y\' = 2)'));
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e = CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e = CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)');
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e IN (CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'))) WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM t_left_array_join_enum LEFT ARRAY JOIN arr AS e WHERE e IN (CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'));
DROP TABLE t_left_array_join_enum;
