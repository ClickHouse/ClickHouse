-- `roundToExp2` of NaN must return NaN, not infinity.

SELECT isNaN(roundToExp2(nan));
SELECT isNaN(roundToExp2(toFloat32(nan)));
SELECT isNaN(roundToExp2(materialize(nan)));
SELECT isNaN(roundToExp2(materialize(toFloat32(nan))));

-- NaN with the sign bit set.
SELECT isNaN(roundToExp2(reinterpretAsFloat64(toUInt64(0xFFF8000000000000))));
SELECT isNaN(roundToExp2(reinterpretAsFloat32(toUInt32(0xFFC00000))));
SELECT isNaN(roundToExp2(materialize(reinterpretAsFloat64(toUInt64(0xFFF8000000000000)))));
SELECT isNaN(roundToExp2(materialize(reinterpretAsFloat32(toUInt32(0xFFC00000)))));

-- NaN with the smallest mantissa.
SELECT isNaN(roundToExp2(materialize(reinterpretAsFloat64(toUInt64(0x7FF0000000000001)))));
SELECT isNaN(roundToExp2(materialize(reinterpretAsFloat32(toUInt32(0x7F800001)))));

SELECT roundToExp2(inf), roundToExp2(-inf), roundToExp2(0.3), roundToExp2(5.5), roundToExp2(-0.3);
SELECT roundToExp2(toFloat32(inf)), roundToExp2(toFloat32(-inf)), roundToExp2(toFloat32(0.3)), roundToExp2(toFloat32(5.5)), roundToExp2(toFloat32(-0.3));
SELECT roundToExp2(materialize(inf)), roundToExp2(materialize(-inf)), roundToExp2(materialize(0.3)), roundToExp2(materialize(5.5)), roundToExp2(materialize(-0.3));
