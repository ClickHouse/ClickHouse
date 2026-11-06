-- The arguments of a parameterized view are constant expressions, serialized with their own data type and read back
-- as the parameter type. Check types whose values are easy to lose on the way: `Bool` from a literal and from an
-- expression, `Float32`, `DateTime64`, `Nullable`, `LowCardinality`, `Array`.

DROP VIEW IF EXISTS v_typed_parameters;
CREATE VIEW v_typed_parameters AS
    SELECT {b:Bool} AS b, {f:Float32} AS f, {d:DateTime64(3, 'UTC')} AS d, {n:Nullable(Int32)} AS n, {s:String} AS s, {a:Array(UInt8)} AS a;

SELECT * FROM v_typed_parameters(b = true, f = toFloat32(1.5), d = toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'), n = NULL, s = 'x', a = [1, 2]);
SELECT * FROM v_typed_parameters(b = (1 = 1), f = 0.1, d = '2024-01-01 00:00:00.5', n = toNullable(-5), s = toLowCardinality('lc'), a = range(3));
SELECT * FROM v_typed_parameters(b = false, f = toFloat32(-0.25), d = toDateTime64('1970-01-02 00:00:00', 3, 'UTC') + INTERVAL 1500 MILLISECOND, n = 7, s = concat('a', 'b'), a = []);

DROP VIEW v_typed_parameters;

-- `EXPLAIN SYNTAX` and `DESCRIBE` of an `SQL SECURITY INVOKER` view substitute the arguments through
-- `analyzeFunctionParamValues`; `EXPLAIN SYNTAX` shows each argument as the value it was serialized to.
DROP VIEW IF EXISTS v_typed_parameters_invoker;
CREATE VIEW v_typed_parameters_invoker SQL SECURITY INVOKER AS
    SELECT {b:Bool} AS b, {f:Float32} AS f, {d:DateTime64(3, 'UTC')} AS d, {n:Nullable(Int32)} AS n, {s:String} AS s, {a:Array(UInt8)} AS a;

EXPLAIN SYNTAX SELECT * FROM v_typed_parameters_invoker(b = toBool(1), f = toFloat32(1.5), d = toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'), n = NULL, s = toLowCardinality('lc'), a = [1, 2]);
EXPLAIN SYNTAX SELECT * FROM v_typed_parameters_invoker(b = false, f = 0.1, d = toDateTime64('1970-01-02 00:00:00', 3, 'UTC') + INTERVAL 1500 MILLISECOND, n = toNullable(-5), s = concat('a', 'b'), a = range(3));
DESCRIBE TABLE v_typed_parameters_invoker(b = true, f = toFloat32(-0.25), d = '2024-01-01 00:00:00.5', n = 7, s = 'x', a = []);

DROP VIEW v_typed_parameters_invoker;

-- A `Dynamic` or `Variant` argument is serialized as the value it holds, the same as that value passed directly, and
-- the same through every entrypoint: `SELECT`, `EXPLAIN SYNTAX` and `DESCRIBE`.
DROP VIEW IF EXISTS v_string_parameter;
DROP VIEW IF EXISTS v_uint_parameter;
CREATE VIEW v_string_parameter SQL SECURITY INVOKER AS SELECT {s:String} AS s;
CREATE VIEW v_uint_parameter SQL SECURITY INVOKER AS SELECT {x:UInt64} AS x;

SELECT * FROM v_string_parameter(s = CAST(toDateTime('2024-01-01 00:00:00', 'UTC') AS Dynamic));
SELECT * FROM v_string_parameter(s = toDateTime('2024-01-01 00:00:00', 'UTC'));
SELECT * FROM v_string_parameter(s = CAST(toFloat32(0.1) AS Dynamic));
SELECT * FROM v_string_parameter(s = toFloat32(0.1));
SELECT * FROM v_string_parameter(s = CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic));
SELECT * FROM v_string_parameter(s = CAST(toUInt8(7) AS Variant(UInt8, String)));
EXPLAIN SYNTAX SELECT * FROM v_string_parameter(s = CAST(toDateTime('2024-01-01 00:00:00', 'UTC') AS Dynamic));
EXPLAIN SYNTAX SELECT * FROM v_string_parameter(s = CAST(toFloat32(0.1) AS Dynamic));
DESCRIBE TABLE v_string_parameter(s = CAST(toFloat32(0.1) AS Dynamic));

SELECT * FROM v_uint_parameter(x = CAST(toUInt32(42) AS Dynamic));
DESCRIBE TABLE v_uint_parameter(x = CAST(toUInt32(42) AS Dynamic));
-- A `DateTime` is serialized as a date-time, which is not a `UInt64`, whether it is passed directly or in a `Dynamic`.
SELECT * FROM v_uint_parameter(x = toDateTime('2024-01-01 00:00:00', 'UTC')); -- { serverError BAD_QUERY_PARAMETER }
SELECT * FROM v_uint_parameter(x = CAST(toDateTime('2024-01-01 00:00:00', 'UTC') AS Dynamic)); -- { serverError BAD_QUERY_PARAMETER }
DESCRIBE TABLE v_uint_parameter(x = CAST(toDateTime('2024-01-01 00:00:00', 'UTC') AS Dynamic)); -- { serverError BAD_QUERY_PARAMETER }

DROP VIEW v_string_parameter;
DROP VIEW v_uint_parameter;
