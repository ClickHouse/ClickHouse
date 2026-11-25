-- dotProduct with a constant argument of another element type: exact results for every tail length, constant on either side.

DROP TABLE IF EXISTS t_dot_mixed;
CREATE TABLE t_dot_mixed (n UInt32, b Array(BFloat16), f Array(Float32), i8 Array(Int8)) ENGINE = Memory;

-- Small dyadic values: every product and every partial sum is exact, so the result does not depend on the summation order.
INSERT INTO t_dot_mixed SELECT
    n,
    arrayMap(i -> toBFloat16(((number * 7 + i * 13) % 31 - 15) / 4), range(n)),
    arrayMap(i -> toFloat32(((number * 11 + i * 5) % 29 - 14) / 8), range(n)),
    arrayMap(i -> toInt8((number * 3 + i * 7) % 21 - 10), range(n))
FROM (SELECT arrayJoin([3, 4, 7, 8, 13, 150]) AS n, number FROM numbers(20));

-- Columns: n, rows, isConstant(c), then the number of rows with a wrong result for
-- BFloat16 x Float64, Float32 x Float64, BFloat16 x Float32, Float32 x BFloat16, Int8 x Float64.
WITH (SELECT arrayMap(i -> ((i * 5) % 17 - 8) / 2, range(3))) AS c
SELECT 3, count(), any(isConstant(c)),
    countIf(dotProduct(b, c) != eb OR dotProduct(c, b) != eb OR dotProduct(b, materialize(c)) != eb),
    countIf(dotProduct(f, c) != ef OR dotProduct(f, materialize(c)) != ef),
    countIf(dotProduct(b, CAST(c AS Array(Float32))) != eb OR dotProduct(b, materialize(CAST(c AS Array(Float32)))) != eb),
    countIf(dotProduct(f, CAST(c AS Array(BFloat16))) != ef OR dotProduct(f, materialize(CAST(c AS Array(BFloat16)))) != ef),
    countIf(dotProduct(i8, c) != ei OR dotProduct(i8, materialize(c)) != ei)
FROM (SELECT *,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, b, c)) AS eb,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, f, c)) AS ef,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, i8, c)) AS ei
    FROM t_dot_mixed WHERE n = 3);

WITH (SELECT arrayMap(i -> ((i * 5) % 17 - 8) / 2, range(4))) AS c
SELECT 4, count(), any(isConstant(c)),
    countIf(dotProduct(b, c) != eb OR dotProduct(c, b) != eb OR dotProduct(b, materialize(c)) != eb),
    countIf(dotProduct(f, c) != ef OR dotProduct(f, materialize(c)) != ef),
    countIf(dotProduct(b, CAST(c AS Array(Float32))) != eb OR dotProduct(b, materialize(CAST(c AS Array(Float32)))) != eb),
    countIf(dotProduct(f, CAST(c AS Array(BFloat16))) != ef OR dotProduct(f, materialize(CAST(c AS Array(BFloat16)))) != ef),
    countIf(dotProduct(i8, c) != ei OR dotProduct(i8, materialize(c)) != ei)
FROM (SELECT *,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, b, c)) AS eb,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, f, c)) AS ef,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, i8, c)) AS ei
    FROM t_dot_mixed WHERE n = 4);

WITH (SELECT arrayMap(i -> ((i * 5) % 17 - 8) / 2, range(7))) AS c
SELECT 7, count(), any(isConstant(c)),
    countIf(dotProduct(b, c) != eb OR dotProduct(c, b) != eb OR dotProduct(b, materialize(c)) != eb),
    countIf(dotProduct(f, c) != ef OR dotProduct(f, materialize(c)) != ef),
    countIf(dotProduct(b, CAST(c AS Array(Float32))) != eb OR dotProduct(b, materialize(CAST(c AS Array(Float32)))) != eb),
    countIf(dotProduct(f, CAST(c AS Array(BFloat16))) != ef OR dotProduct(f, materialize(CAST(c AS Array(BFloat16)))) != ef),
    countIf(dotProduct(i8, c) != ei OR dotProduct(i8, materialize(c)) != ei)
FROM (SELECT *,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, b, c)) AS eb,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, f, c)) AS ef,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, i8, c)) AS ei
    FROM t_dot_mixed WHERE n = 7);

WITH (SELECT arrayMap(i -> ((i * 5) % 17 - 8) / 2, range(8))) AS c
SELECT 8, count(), any(isConstant(c)),
    countIf(dotProduct(b, c) != eb OR dotProduct(c, b) != eb OR dotProduct(b, materialize(c)) != eb),
    countIf(dotProduct(f, c) != ef OR dotProduct(f, materialize(c)) != ef),
    countIf(dotProduct(b, CAST(c AS Array(Float32))) != eb OR dotProduct(b, materialize(CAST(c AS Array(Float32)))) != eb),
    countIf(dotProduct(f, CAST(c AS Array(BFloat16))) != ef OR dotProduct(f, materialize(CAST(c AS Array(BFloat16)))) != ef),
    countIf(dotProduct(i8, c) != ei OR dotProduct(i8, materialize(c)) != ei)
FROM (SELECT *,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, b, c)) AS eb,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, f, c)) AS ef,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, i8, c)) AS ei
    FROM t_dot_mixed WHERE n = 8);

WITH (SELECT arrayMap(i -> ((i * 5) % 17 - 8) / 2, range(13))) AS c
SELECT 13, count(), any(isConstant(c)),
    countIf(dotProduct(b, c) != eb OR dotProduct(c, b) != eb OR dotProduct(b, materialize(c)) != eb),
    countIf(dotProduct(f, c) != ef OR dotProduct(f, materialize(c)) != ef),
    countIf(dotProduct(b, CAST(c AS Array(Float32))) != eb OR dotProduct(b, materialize(CAST(c AS Array(Float32)))) != eb),
    countIf(dotProduct(f, CAST(c AS Array(BFloat16))) != ef OR dotProduct(f, materialize(CAST(c AS Array(BFloat16)))) != ef),
    countIf(dotProduct(i8, c) != ei OR dotProduct(i8, materialize(c)) != ei)
FROM (SELECT *,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, b, c)) AS eb,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, f, c)) AS ef,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, i8, c)) AS ei
    FROM t_dot_mixed WHERE n = 13);

WITH (SELECT arrayMap(i -> ((i * 5) % 17 - 8) / 2, range(150))) AS c
SELECT 150, count(), any(isConstant(c)),
    countIf(dotProduct(b, c) != eb OR dotProduct(c, b) != eb OR dotProduct(b, materialize(c)) != eb),
    countIf(dotProduct(f, c) != ef OR dotProduct(f, materialize(c)) != ef),
    countIf(dotProduct(b, CAST(c AS Array(Float32))) != eb OR dotProduct(b, materialize(CAST(c AS Array(Float32)))) != eb),
    countIf(dotProduct(f, CAST(c AS Array(BFloat16))) != ef OR dotProduct(f, materialize(CAST(c AS Array(BFloat16)))) != ef),
    countIf(dotProduct(i8, c) != ei OR dotProduct(i8, materialize(c)) != ei)
FROM (SELECT *,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, b, c)) AS eb,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, f, c)) AS ef,
    arraySum(arrayMap((x, y) -> toFloat64(x) * y, i8, c)) AS ei
    FROM t_dot_mixed WHERE n = 150);

DROP TABLE t_dot_mixed;
