-- Without GROUP BY, `boundingRatio` converts its arguments in chunks (`addBatchSinglePlace`), with GROUP BY
-- it reads them row by row (`add`). Both must give the same result for every argument type, also across
-- chunk boundaries (1000 rows, values not ordered by `x`).

DROP TABLE IF EXISTS t_bounding_ratio;
CREATE TABLE t_bounding_ratio (x UInt64, y Int64, k UInt8) ENGINE = Memory;
INSERT INTO t_bounding_ratio SELECT (number * 37) % 200, 3 * ((number * 37) % 200) - 100, 0 FROM numbers(1000);

SELECT 'UInt8', (SELECT boundingRatio(toUInt8(x), toUInt8(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toUInt8(x), toUInt8(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'UInt16', (SELECT boundingRatio(toUInt16(x), toUInt16(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toUInt16(x), toUInt16(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'UInt32', (SELECT boundingRatio(toUInt32(x), toUInt32(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toUInt32(x), toUInt32(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'UInt64', (SELECT boundingRatio(x, toUInt64(y)) FROM t_bounding_ratio), (SELECT boundingRatio(x, toUInt64(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'Int8', (SELECT boundingRatio(toInt8(x), toInt8(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toInt8(x), toInt8(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'Int16', (SELECT boundingRatio(toInt16(x), toInt16(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toInt16(x), toInt16(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'Int32', (SELECT boundingRatio(toInt32(x), toInt32(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toInt32(x), toInt32(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'Int64', (SELECT boundingRatio(toInt64(x), y) FROM t_bounding_ratio), (SELECT boundingRatio(toInt64(x), y) FROM t_bounding_ratio GROUP BY k);
SELECT 'Float32', (SELECT boundingRatio(toFloat32(x) / 3, toFloat32(y) / 7) FROM t_bounding_ratio), (SELECT boundingRatio(toFloat32(x) / 3, toFloat32(y) / 7) FROM t_bounding_ratio GROUP BY k);
SELECT 'Float64', (SELECT boundingRatio(x / 3, y / 7) FROM t_bounding_ratio), (SELECT boundingRatio(x / 3, y / 7) FROM t_bounding_ratio GROUP BY k);

-- Types without a typed conversion fall back to `getFloat64`.
SELECT 'Date', (SELECT boundingRatio(toDate(x), y) FROM t_bounding_ratio), (SELECT boundingRatio(toDate(x), y) FROM t_bounding_ratio GROUP BY k);
SELECT 'DateTime', (SELECT boundingRatio(toDateTime(x, 'UTC'), y) FROM t_bounding_ratio), (SELECT boundingRatio(toDateTime(x, 'UTC'), y) FROM t_bounding_ratio GROUP BY k);
SELECT 'Decimal32', (SELECT boundingRatio(toDecimal32(x, 2), toDecimal32(y, 3)) FROM t_bounding_ratio), (SELECT boundingRatio(toDecimal32(x, 2), toDecimal32(y, 3)) FROM t_bounding_ratio GROUP BY k);
SELECT 'UInt128', (SELECT boundingRatio(toUInt128(x), toInt256(y)) FROM t_bounding_ratio), (SELECT boundingRatio(toUInt128(x), toInt256(y)) FROM t_bounding_ratio GROUP BY k);
SELECT 'Nullable', (SELECT boundingRatio(toNullable(x), y) FROM t_bounding_ratio), (SELECT boundingRatio(toNullable(x), y) FROM t_bounding_ratio GROUP BY k);

-- The -If combinator.
SELECT 'If', (SELECT boundingRatioIf(x, y, x % 3 = 1) FROM t_bounding_ratio), (SELECT boundingRatioIf(x, y, x % 3 = 1) FROM t_bounding_ratio GROUP BY k);

DROP TABLE t_bounding_ratio;

-- `randomString` with a constant and with a non-constant length.
SELECT DISTINCT length(randomString(7)) FROM numbers(1000);
SELECT sum(length(randomString(number % 4))), arraySort(groupUniqArray(length(randomString(number % 4)))) = [0, 1, 2, 3] FROM numbers(1000);
SELECT DISTINCT length(randomString(0)) FROM numbers(10);
