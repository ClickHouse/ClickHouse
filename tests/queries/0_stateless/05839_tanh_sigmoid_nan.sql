-- tanh and sigmoid return NaN for a NaN argument (the approximate fastops kernel returned -1 and 0 on x86).
SELECT isNaN(tanh(nan)), isNaN(sigmoid(nan)), isNaN(tanh(toFloat32(nan))), isNaN(sigmoid(toFloat32(nan)));
SELECT isNaN(tanh(toNullable(nan))), isNaN(sigmoid(toLowCardinality(nan)));
-- a NaN with payload bits (0x7ff8000000000001)
SELECT isNaN(tanh(reinterpretAsFloat64(toUInt64(9221120237041090561)))), isNaN(sigmoid(reinterpretAsFloat64(toUInt64(9221120237041090561))));
-- vector path: NaN inside full SIMD batches and in the tail
SELECT countIf(isNaN(tanh(x))), countIf(isNaN(sigmoid(x))), countIf(isNaN(tanh(toFloat32(x)))), countIf(isNaN(sigmoid(toFloat32(x))))
FROM (SELECT if(number % 7 = 0, nan, toFloat64(number) - 50) AS x FROM numbers(100));
SELECT x, round(tanh(x), 5), round(sigmoid(x), 5) FROM (SELECT arrayJoin([nan, -1, 0, 0.5, inf, -inf]) AS x);
