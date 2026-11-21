-- JSON functions accept a `LowCardinality(Nullable(T))` argument and treat it as `Nullable(T)`.

SELECT JSONExtractString(toLowCardinality(toNullable('{"a":"b"}')), 'a');
SELECT JSONExtractInt('[1]', toLowCardinality(toNullable(1)));
SELECT JSONLength(toLowCardinality(toNullable('[1,2,3]')));

SELECT toTypeName(JSONExtractString(toLowCardinality(toNullable('{"a":"b"}')), 'a'));

DROP TABLE IF EXISTS t_json_low_cardinality_nullable;
CREATE TABLE t_json_low_cardinality_nullable (id UInt8, j LowCardinality(Nullable(String))) ENGINE = Memory;
INSERT INTO t_json_low_cardinality_nullable VALUES (1, '{"a":"b"}'), (2, NULL);
SELECT JSONExtractString(j, 'a') FROM t_json_low_cardinality_nullable ORDER BY id;
DROP TABLE t_json_low_cardinality_nullable;

-- The single-wrapper cases keep their previous results.
SELECT JSONExtractString(toNullable('{"a":"b"}'), 'a');
SELECT JSONExtractString(toLowCardinality('{"a":"b"}'), 'a');
SELECT toTypeName(JSONExtractString(toNullable('{"a":"b"}'), 'a'));
SELECT toTypeName(JSONExtractString(toLowCardinality('{"a":"b"}'), 'a'));
