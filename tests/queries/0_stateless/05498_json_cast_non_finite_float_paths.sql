-- A conversion to JSON must keep inf and nan of floating-point paths.
SELECT j.a, j.b, j.c FROM (SELECT CAST('{"a":"-inf","b":"nan","c":"inf"}'::JSON(a Float64, b Float32, c Nullable(Float64)), 'JSON(a Float64, b Float32, c Nullable(Float64), z String)') AS j);
SELECT j.a FROM (SELECT CAST('{"a":["inf","-inf",1]}'::JSON(a Array(Float64)), 'JSON(a Array(Float64), z String)') AS j);
SELECT j.a, j.b FROM (SELECT CAST(map('a', inf, 'b', -inf), 'JSON(a Float64, b Float64)') AS j);
SELECT j.a, dynamicType(j.a) FROM (SELECT CAST('{"a":"inf"}'::JSON(a Float64), 'JSON') AS j);
SELECT CAST('{"a":"inf"}'::JSON(a Float64), 'JSON(a Int64)'); -- { serverError INCORRECT_DATA }
SELECT accurateCastOrNull('{"a":"inf"}'::JSON(a Float64), 'JSON(a Int64)');
SELECT CAST('{"a":"inf"}'::JSON(a Float64), 'JSON(a Decimal64(2))'); -- { serverError INCORRECT_DATA }
SELECT accurateCastOrNull('{"a":"inf"}'::JSON(a Float64), 'JSON(a Decimal64(2))');

DROP TABLE IF EXISTS t_json_non_finite;
CREATE TABLE t_json_non_finite (id UInt64, data JSON(a Float64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_non_finite VALUES (0, '{"a":"-inf"}'), (1, '{"a":1}'), (2, '{"a":"inf"}'), (3, '{"a":"nan"}');
ALTER TABLE t_json_non_finite MODIFY COLUMN data JSON(a Float64, c String) SETTINGS mutations_sync = 2;
SELECT id, data.a FROM t_json_non_finite ORDER BY id;
DROP TABLE t_json_non_finite;
