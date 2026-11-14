SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_check_scaled_decimal_left;
CREATE TABLE squality_check_scaled_decimal_left
(
    d Decimal64(17),
    x Float64,
    CONSTRAINT c CHECK d IN [x]
) ENGINE = Memory;
INSERT INTO squality_check_scaled_decimal_left VALUES ('1.00000762939453125', 1.00000762939453125);
INSERT INTO squality_check_scaled_decimal_left VALUES ('1.00000762939453120', 1.00000762939453125); -- { serverError VIOLATED_CONSTRAINT }
SELECT count(), sum(d IN [x]) FROM squality_check_scaled_decimal_left;
DROP TABLE squality_check_scaled_decimal_left;

DROP TABLE IF EXISTS squality_check_scaled_float_left;
CREATE TABLE squality_check_scaled_float_left
(
    x Float64,
    d Decimal64(17),
    CONSTRAINT c CHECK x IN [d]
) ENGINE = Memory;
INSERT INTO squality_check_scaled_float_left VALUES (1.00000762939453125, '1.00000762939453125');
INSERT INTO squality_check_scaled_float_left VALUES (1.00000762939453125, '1.00000762939453120'); -- { serverError VIOLATED_CONSTRAINT }
SELECT count(), sum(x IN [d]) FROM squality_check_scaled_float_left;
DROP TABLE squality_check_scaled_float_left;
