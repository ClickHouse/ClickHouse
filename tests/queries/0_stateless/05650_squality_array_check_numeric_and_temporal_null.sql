SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_check_array_nan;
CREATE TABLE squality_check_array_nan
(
    x BFloat16,
    y BFloat16,
    CONSTRAINT c CHECK x IN [y]
) ENGINE = Memory;
INSERT INTO squality_check_array_nan VALUES ('nan', 'nan'); -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_check_array_nan;

SELECT x IN [y]
FROM values('x BFloat16, y BFloat16', ('nan', 'nan'));

DROP TABLE IF EXISTS squality_check_array_decimal_float;
CREATE TABLE squality_check_array_decimal_float
(
    x Decimal128(0),
    y Float64,
    CONSTRAINT c CHECK x IN [y]
) ENGINE = Memory;
INSERT INTO squality_check_array_decimal_float VALUES ('1', 1);
SELECT x IN [y] FROM squality_check_array_decimal_float;
DROP TABLE squality_check_array_decimal_float;

DROP TABLE IF EXISTS squality_check_array_decimal_distinct;
CREATE TABLE squality_check_array_decimal_distinct
(
    x Decimal128(0),
    y Float64,
    CONSTRAINT c CHECK x NOT IN [y]
) ENGINE = Memory;
INSERT INTO squality_check_array_decimal_distinct VALUES ('9007199254740993', 9007199254740992);
SELECT x NOT IN [y] FROM squality_check_array_decimal_distinct;
DROP TABLE squality_check_array_decimal_distinct;

SELECT a IN ([1], v), a NOT IN ([1], v)
FROM values('a Array(UInt8), v Nullable(DateTime)', ([1], NULL), ([2], NULL));

SELECT a IN (v), a NOT IN (v)
FROM values('a Array(UInt8), v Nullable(DateTime)', ([1], NULL));

SELECT a IN (materialize(CAST(NULL AS Nullable(DateTime))))
FROM values('a Array(UInt8)', ([1]));
