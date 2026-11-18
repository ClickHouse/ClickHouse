-- Float32 and BFloat16 values converted to 128- and 256-bit integers, and Float32 values converted to Decimal128/Decimal256.

SELECT toInt128(toFloat32(1e20)) = toInt128(toFloat64(toFloat32(1e20))), toInt256(toFloat32(-1e20)) = toInt256(toFloat64(toFloat32(-1e20)));
SELECT toUInt128(toFloat32(1e19)), toUInt256(toFloat32(1e19));
SELECT toInt128(toFloat32(-0.5)), toInt256(toFloat32(-0.5)), toDecimal128(toFloat32(-0.0001), 3);
SELECT toInt128(toBFloat16(-5)), toInt256(toBFloat16(-5)), toInt128(toBFloat16(1e20)) = toInt128(toFloat64(toBFloat16(1e20)));
SELECT toUInt128(toFloat32(-0.5)) = toUInt128(toFloat64(-0.5)), toUInt256(toFloat32(-0.5)) = toUInt256(toFloat64(-0.5)), toUInt128(toBFloat16(-5)) = toUInt128(toFloat64(toBFloat16(-5))), toUInt256(toBFloat16(-5)) = toUInt256(toFloat64(toBFloat16(-5)));
SELECT abs(toFloat64(CAST(toFloat32(1.5) AS Decimal(38, 20))) - 1.5) < 1e-6, abs(toFloat64(toDecimal128(toFloat32(10), 31)) - 10) < 1e-5, abs(toFloat64(toDecimal256(toFloat32(-123.25), 30)) + 123.25) < 1e-4;

DROP TABLE IF EXISTS t_float32_set_decimal128_key;
CREATE TABLE t_float32_set_decimal128_key (id Decimal(38, 31)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_float32_set_decimal128_key SELECT number + 1 FROM numbers(100);
SELECT count() FROM t_float32_set_decimal128_key WHERE id IN (SELECT toFloat32(10));
DROP TABLE t_float32_set_decimal128_key;
