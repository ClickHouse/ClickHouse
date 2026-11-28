CREATE TEMPORARY TABLE float_integer_formatting (id UInt8, f32 Float32, f64 Float64);

-- Exercise the integer paths around the significand and signed-integer limits of `Float32` and `Float64`.
INSERT INTO float_integer_formatting VALUES
    (1, '0', '0'),
    (2, '-0', '-0'),
    (3, '1', '1'),
    (4, '-1', '-1'),
    (5, '8388607', '4503599627370495'),
    (6, '8388608', '4503599627370496'),
    (7, '16777215', '9007199254740991'),
    (8, '16777216', '9007199254740992'),
    (9, '16777218', '9007199254740994'),
    (10, '33554432', '18014398509481984'),
    (11, '-16777216', '-9007199254740992'),
    (12, '-33554432', '-18014398509481984'),
    (13, '1000000064', '1000000000000000128'),
    (14, '-1000000064', '-1000000000000000128'),
    (15, '2147483520', '9223372036854774784'),
    (16, '-2147483520', '-9223372036854774784'),
    (17, '2147483648', '9223372036854775808'),
    (18, '-2147483648', '-9223372036854775808'),
    (19, '1.5', '1.5'),
    (20, '-1.5', '-1.5'),
    (21, '8388607.5', '4503599627370495.5'),
    (22, '1.23e20', '1e20'),
    (23, '1e30', '1e100'),
    (24, '1e-30', '1e-100'),
    (25, 'inf', 'inf'),
    (26, '-inf', '-inf'),
    (27, 'nan', 'nan');

SELECT f32, f64 FROM float_integer_formatting ORDER BY id
SETTINGS output_format_float_precision = 0, output_format_always_write_decimal_point_in_float_and_decimal = 0;

-- Integer output gains exactly one point; fractional, exponential and non-finite output stays unchanged.
SELECT f32, f64 FROM float_integer_formatting ORDER BY id
SETTINGS output_format_float_precision = 0, output_format_always_write_decimal_point_in_float_and_decimal = 1;

-- `JSONEachRow` must keep valid number tokens even when the decimal-point setting is enabled.
SELECT f32, f64 FROM float_integer_formatting WHERE id IN (3, 10, 13, 17, 22, 23, 26) ORDER BY id
SETTINGS output_format_float_precision = 0, output_format_always_write_decimal_point_in_float_and_decimal = 1,
         output_format_json_quote_64bit_floats = 0, output_format_json_quote_denormals = 0
FORMAT JSONEachRow;

DROP TEMPORARY TABLE float_integer_formatting;
