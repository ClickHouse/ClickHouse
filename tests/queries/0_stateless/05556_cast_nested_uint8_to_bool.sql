-- CAST to a container of Bool normalizes nested UInt8 values to 0 or 1, as the scalar CAST does.

SELECT CAST([toUInt8(2), 0, 5] AS Array(Bool)) AS a, arrayMap(x -> toUInt8(x), a), countEqual(a, true);
SELECT count() FROM (SELECT CAST(materialize([toUInt8(2)]) AS Array(Bool)) AS a) WHERE a[1] = true;
SELECT has(CAST([toUInt8(2)] AS Array(Bool)), true);
SELECT CAST([toNullable(toUInt8(2)), NULL] AS Array(Nullable(Bool))) AS a, arrayMap(x -> toUInt8(x), a);

SET allow_suspicious_low_cardinality_types = 1;
SELECT arrayMap(x -> toUInt8(x), CAST([toLowCardinality(toUInt8(1)), toLowCardinality(toUInt8(2)), toLowCardinality(toUInt8(0))] AS Array(LowCardinality(Bool))));

SELECT toUInt8(CAST([[toUInt8(3)]] AS Array(Array(Bool)))[1][1]);
SELECT CAST((toUInt8(2), 'x') AS Tuple(Bool, String)) AS t, toUInt8(t.1), t.1 = true;
SELECT CAST(map(1, toUInt8(2)) AS Map(UInt8, Bool)) AS m, m[1] = true, toUInt8(m[1]);
SELECT toUInt8(mapKeys(CAST(map(toUInt8(2), 1) AS Map(Bool, UInt8)))[1]);
SELECT CAST([toUInt8(2)]::Dynamic AS Array(Bool)) AS a, toUInt8(a[1]);
SELECT toUInt8(CAST(CAST([toUInt8(2)] AS Variant(Array(UInt8), String)) AS Array(Bool))[1]);
SELECT CAST(CAST('{"a":2}' AS JSON(a UInt8)) AS JSON(a Bool)) AS j, toTypeName(j.a), toUInt8(j.a), j.a = true;
SELECT toUInt8(k.a), dynamicType(k.b), dynamicType(k.c) FROM (SELECT CAST('{"a":2,"b":1.0,"c":[1.0,2.0]}'::JSON(a UInt8) AS JSON(a Bool)) AS k);
SELECT j, toUInt8(tupleElement(j.a, 1)) FROM (SELECT accurateCastOrNull(materialize('{"a":[2]}'::JSON(a Tuple(UInt8))), 'JSON(a Tuple(Bool))') AS j);
SELECT j, toUInt8(tupleElement(j.a[1], 1)) FROM (SELECT accurateCastOrNull('{"a":[[2]]}'::JSON(a Array(Tuple(UInt8))), 'JSON(a Array(Tuple(Bool)))') AS j);
SELECT j, toUInt8(tupleElement(j.a, 1)) FROM (SELECT accurateCastOrDefault(materialize('{"a":[2]}'::JSON(a Tuple(UInt8))), 'JSON(a Tuple(Bool))') AS j);
SELECT toUInt8(accurateCast([toUInt8(2)], 'Array(Bool)')[1]);

DROP TABLE IF EXISTS t;
CREATE TABLE t (a Array(UInt8)) ENGINE = Memory;
INSERT INTO t VALUES ([2]);
SELECT count() FROM t WHERE CAST(a AS Array(Bool))[1] = true;
DROP TABLE t;

-- Casts whose result does not change.
SELECT CAST([toUInt8(2)] AS Array(UInt8));
SELECT CAST([true, false] AS Array(UInt8));
SELECT CAST(toUInt8(2) AS Bool) AS b, toUInt8(b);
SELECT CAST([(1.5, 2.5)]::Array(Point) AS Array(Tuple(Float64, Float64))) AS a, toTypeName(a);
SELECT CAST([[(1., 2.)]]::Polygon AS Array(Array(Tuple(Float64, Float64))));
SELECT CAST([toUInt8(2)]::Array(SimpleAggregateFunction(any, UInt8)) AS Array(UInt8));
SELECT toUInt8(k.a), dynamicType(k.b) FROM (SELECT CAST('{"a":true,"b":1.0}'::JSON(a Bool) AS JSON(a UInt8)) AS k);
SELECT CAST([CAST([(1.5, 2.5)]::Array(Point) AS Variant(Array(Point), String))] AS Array(Variant(Array(Tuple(Float64, Float64)), String)));
