-- round, roundBankers, floor, ceil and trunc on floats where x * 10^N overflows or |N| >= 20.
SELECT round(1e300, 10), roundBankers(1e300, 10), floor(-1e300, 10), ceil(1e300, 10), trunc(-1e300, 10);
SELECT round(toFloat32(1e30), 9), floor(toFloat32(-1e30), 9), round(toBFloat16(1e30), 10);
SELECT round(1.6e-20, 20), floor(1.6e-20, 20), ceil(1.4e-20, 20), round(toFloat32(1.6e-20), 20), floor(toFloat32(4.248807726546337e-14), 20);
SELECT floor(1.23456789e25, -20), ceil(1.23456789e25, -20), round(1.23456789e25, -24), trunc(-1.23456789e25, -20), floor(toFloat32(1.23456789e25), -20), round(toBFloat16(-1.4955e38), -38);
SELECT floor(1.7e308, -308), round(7e307, -308), round(1.7e308, -308);
SELECT round(1., 400), round(-0., 400), round(1.234567890123e-300, 309), round(6e-310, 309), round(toFloat32(1.5), 39), round(toFloat32(0), 39), round(toFloat32(1.234567e-38), 39);
SELECT round(1., -400), trunc(-1., -400), floor(-1., -400), ceil(1., -400), ceil(-1., -400), ceil(toFloat32(1), -39), round(inf, -400), round(nan, -400);
SELECT ceil(1e-300, -30), floor(-1e-300, -30);
SELECT round(6.04232e56, 20), floor(1.74686e55, -20), round(2.4554477816543998e150, 20);
-- x * 10^N rounds onto an integer from the wrong side; floor(x) <= x <= ceil(x) still holds.
SELECT ceil(6.385506180058851e-264, 278), floor(-6.03263106e-18, 26), ceil(-6.36297676e44, -36);
SELECT x, n, round(x, n), floor(x, n), ceil(x, n) FROM values('x Float64, n Int16', (1e300, 10), (1.6e-20, 20), (1.23456789e25, -20), (1., 400), (-1., -400), (1e-300, -30));
SELECT x, round(x, 10), round(x, 20), floor(x, -20), ceil(x, -400), y, round(y, 9), floor(y, 20) FROM values('x Float64, y Float32', (1e300, 1e30), (1.6e-20, 1.6e-20), (1.23456789e25, 1.23456789e25), (-1., -1.));
SELECT round(123.456, 2), floor(-1.5), roundBankers(2.5), ceil(123.45, -1);
