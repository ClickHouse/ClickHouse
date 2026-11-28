-- an `Enum` and a string in arrays compare by the name: '10' < '2' < '3', the folding must not use the values

DROP TABLE IF EXISTS t_enum_array_fold;
DROP TABLE IF EXISTS t_enum_tuple_array_fold;
DROP TABLE IF EXISTS t_enum_map_fold;

SET optimize_and_compare_chain = 0;

CREATE TABLE t_enum_array_fold (a Array(Enum8('10' = 10, '2' = 2, '3' = 3))) ENGINE = Memory;
INSERT INTO t_enum_array_fold VALUES (['2']);
SELECT 'array', count() FROM t_enum_array_fold WHERE a < ['3'] AND a > ['10'] SETTINGS optimize_redundant_comparisons = 0;
SELECT 'array', count() FROM t_enum_array_fold WHERE a < ['3'] AND a > ['10'] SETTINGS optimize_redundant_comparisons = 1;
SELECT 'array chain', count() FROM t_enum_array_fold WHERE a < ['3'] AND a > ['10'] SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1;

CREATE TABLE t_enum_tuple_array_fold (a Tuple(Array(Enum8('10' = 10, '2' = 2, '3' = 3)))) ENGINE = Memory;
INSERT INTO t_enum_tuple_array_fold VALUES (tuple(['2']));
SELECT 'tuple of array', count() FROM t_enum_tuple_array_fold WHERE a < tuple(['3']) AND a > tuple(['10']) SETTINGS optimize_redundant_comparisons = 0;
SELECT 'tuple of array', count() FROM t_enum_tuple_array_fold WHERE a < tuple(['3']) AND a > tuple(['10']) SETTINGS optimize_redundant_comparisons = 1;

CREATE TABLE t_enum_map_fold (m Map(String, Enum8('10' = 10, '2' = 2, '3' = 3))) ENGINE = Memory;
INSERT INTO t_enum_map_fold VALUES (map('k', '2'));
SELECT 'map', count() FROM t_enum_map_fold WHERE m < map('k', '3') AND m > map('k', '10') SETTINGS optimize_redundant_comparisons = 0;
SELECT 'map', count() FROM t_enum_map_fold WHERE m < map('k', '3') AND m > map('k', '10') SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE t_enum_array_fold;
DROP TABLE t_enum_tuple_array_fold;
DROP TABLE t_enum_map_fold;
