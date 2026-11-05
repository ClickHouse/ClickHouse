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
