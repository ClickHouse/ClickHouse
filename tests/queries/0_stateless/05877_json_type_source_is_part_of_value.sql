-- The JSON text stored for a type with `with_source=1` is a part of the value: objects with equal paths and values
-- but different text are different values in comparison, hashing and aggregation.

SELECT 'comparison';
SELECT '{"a":1}'::JSON(with_source=1) = '{ "a" : 1 }'::JSON(with_source=1), '{"a":1}'::JSON(with_source=1) = '{"a":1}'::JSON(with_source=1);
-- Objects are compared first, the text only orders equal objects.
SELECT '{ "a" : 2 }'::JSON(with_source=1) < '{"a":1}'::JSON(with_source=1), '{ "a" : 1 }'::JSON(with_source=1) < '{"a":1}'::JSON(with_source=1);

DROP TABLE IF EXISTS t_json_source_value;
CREATE TABLE t_json_source_value (id UInt64, json JSON(with_source=1)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_source_value VALUES (1, '{"a":1}'), (2, '{ "a" : 1 }'), (3, '{"a":1}'), (4, '{"a":2}');

SELECT 'ORDER BY';
SELECT id, json.__source FROM t_json_source_value ORDER BY json, id;

SELECT 'GROUP BY keeps the text of each group';
SELECT j.__source, c FROM (SELECT json AS j, count() AS c FROM t_json_source_value GROUP BY json) ORDER BY j.__source;

SELECT 'DISTINCT';
SELECT j.__source FROM (SELECT DISTINCT json AS j FROM t_json_source_value) ORDER BY j.__source;

SELECT 'uniqExact and groupUniqArray';
SELECT uniqExact(json) FROM t_json_source_value;
SELECT arraySort(g.__source) FROM (SELECT groupUniqArray(json) AS g FROM t_json_source_value);

SELECT 'JOIN';
SELECT l.id, r.id FROM t_json_source_value AS l INNER JOIN t_json_source_value AS r ON l.json = r.json ORDER BY l.id, r.id;

DROP TABLE t_json_source_value;

SELECT 'insert deduplication';
DROP TABLE IF EXISTS t_json_source_dedup;
CREATE TABLE t_json_source_dedup (json JSON(with_source=1)) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t_json_source_dedup VALUES ('{"a":1}');
INSERT INTO t_json_source_dedup VALUES ('{ "a" : 1 }');
INSERT INTO t_json_source_dedup VALUES ('{"a":1}');
SELECT json.__source FROM t_json_source_dedup ORDER BY json.__source;
DROP TABLE t_json_source_dedup;

SELECT 'nested values keep their source in conversions';
-- A value that gains the source gets the text of its object with the text of the nested values in it.
WITH CAST(materialize('{"a":{"z":1,"b":2}}'::JSON(a JSON(with_source=1))) AS JSON(with_source=1, a JSON(with_source=1))) AS j
SELECT j.__source, j.a.__source;
WITH CAST(CAST(materialize('{"a":{"z":1,"b":2}}'::JSON(a JSON(with_source=1))) AS JSON(with_source=1, a JSON(with_source=1))) AS JSON(with_source=1, max_dynamic_paths=1, a JSON(with_source=1))) AS j
SELECT j.__source, j.a.__source;
WITH CAST(materialize('{"a":{"z":1,"b":2}}'::JSON(a JSON(with_source=1))) AS JSON(a JSON(with_source=1), c UInt32)) AS j
SELECT j.a.__source SETTINGS json_use_optimized_type_conversion = 0;

SELECT 'a default has the JSON text of the default';
SELECT firstNonDefault(materialize('{ }'::JSON(with_source=1)), '{"a":1}'::JSON(with_source=1)).__source;
SELECT firstNonDefault(materialize('{}'::JSON(with_source=1)), '{"a":1}'::JSON(with_source=1)).__source;
SELECT firstNonDefault(materialize('{ "e" : "x" }'::JSON(with_source=1, e Enum8('x' = 1, 'y' = 2))), '{"e":"y"}'::JSON(with_source=1, e Enum8('x' = 1, 'y' = 2))).__source;
SELECT tupleElement(tupleElement(firstNonDefault(materialize(tuple('{ }'::JSON(with_source=1))), tuple('{"a":1}'::JSON(with_source=1))), 1), '__source');

SELECT 'the text created for binary input is parsed back to the same value';
SELECT j.f, j.__source, CAST(j AS JSON(with_source=1, f Float64, max_dynamic_paths=1)).f
FROM format(RowBinary, 'j JSON(with_source=1, f Float64)', unhex('010166000000000000f87f'))
SETTINGS input_format_binary_read_json_as_string = 0;
SELECT j.__source, dynamicType(CAST(j AS JSON(with_source=1, max_dynamic_paths=1)).a)
FROM format(RowBinary, 'j JSON(with_source=1)', formatRow('RowBinary', '{"a" : 42}'::JSON))
SETTINGS input_format_binary_read_json_as_string = 0, output_format_json_quote_64bit_integers = 1;
