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
