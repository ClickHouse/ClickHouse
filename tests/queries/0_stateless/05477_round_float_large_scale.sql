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
-- x * 10^N can round onto an integer from the wrong side; floor(x) <= x <= ceil(x) still holds.
SELECT ceil(6.385506180058851e-264, 278), floor(-6.03263106e-18, 26), ceil(-6.36297676e44, -36);
SELECT floor(8.048537127250599e-10, 23), ceil(6.782840434034981e-50, 64), floor(8.401063861109889e46, -32), trunc(-6.704123565980179e155, -141);
SELECT x, n, round(x, n), floor(x, n), ceil(x, n) FROM values('x Float64, n Int16', (1e300, 10), (1.6e-20, 20), (1.23456789e25, -20), (1., 400), (-1., -400), (1e-300, -30));
SELECT x, round(x, 10), round(x, 20), floor(x, -20), ceil(x, -400), y, round(y, 9), floor(y, 20) FROM values('x Float64, y Float32', (1e300, 1e30), (1.6e-20, 1.6e-20), (1.23456789e25, 1.23456789e25), (-1., -1.));
-- Negative N with |N| <= 19: x / 10^N * 10^N overflows at the type max.
SELECT floor(1.7976931348623157e308, -6), round(1.7976931348623157e308, -6), floor(toFloat32(3.4028235e38), -2), round(toFloat32(3.4028235e38), -2);
SELECT x, floor(x, -6), y, round(y, -2) FROM values('x Float64, y Float32', (1.7976931348623157e308, 3.4028234663852886e38));
SELECT round(123.456, 2), floor(-1.5), roundBankers(2.5), ceil(123.45, -1);
-- floor, ceil and trunc return a value on the 10^-N grid unchanged, also where 10^N is inexact (|N| >= 23).
SELECT floor(6e-23, 23), trunc(3e-23, 23), ceil(1.5e24, -23), floor(5e26, -25), floor(6e-23, materialize(23));
SELECT floor(7e-22, 22), trunc(4e-22, 22), ceil(1.7e23, -22), floor(5e24, -22);
SELECT floor(1e-213, 213), round(-2.1e-81, 82), ceil(7e-210, 209), floor(6e-300, 309), ceil(3e-323, 307), ceil(2.4e-309, 310), floor(3e23, -23);
SELECT floor(-0., 23), ceil(-3e-24, 23), floor(-0., -23), ceil(-3e22, -23);
SELECT x, floor(x, 23), trunc(x, 23), y, ceil(y, -23), floor(y, -25) FROM values('x Float64, y Float64', (6e-23, 1.5e24), (3e-23, 5e26));
SELECT x, n, floor(x, n), ceil(x, n), round(x, n) FROM values('x Float64, n Int16', (6e-23, 23), (1.5e24, -23), (4e-308, 309), (3e-323, 307), (3e23, -23));
SELECT floor(1e23, -23), trunc(1e23, -23), floor(2e23, -23), floor(4e23, -23), floor(3e-25, 25), ceil(2.7e-22, 23);
SELECT floor(1e-21, 21), ceil(3e-20, 20), floor(5e-22, 22), ceil(9.5e21, -20), floor(3.1e23, -22);
SELECT x, n, floor(x, n), trunc(x, n), ceil(x, n) FROM values('x Float64, n Int16', (1e23, -23), (3e-25, 25), (2.7e-22, 23), (1e-21, 21));
