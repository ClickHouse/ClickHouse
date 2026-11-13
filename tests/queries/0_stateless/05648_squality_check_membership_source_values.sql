SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_check_numeric_membership;
CREATE TABLE squality_check_numeric_membership
(
    x Int64,
    y Float64,
    CONSTRAINT c CHECK x IN (y, NULL)
) ENGINE = Memory;
INSERT INTO squality_check_numeric_membership VALUES (0, -0.6); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO squality_check_numeric_membership VALUES (1, 1);
SELECT x, toInt64(y), x IN (y, NULL) FROM squality_check_numeric_membership;
DROP TABLE squality_check_numeric_membership;

DROP TABLE IF EXISTS squality_check_string_membership;
CREATE TABLE squality_check_string_membership
(
    x UInt8,
    CONSTRAINT c CHECK x NOT IN materialize('256')
) ENGINE = Memory;
INSERT INTO squality_check_string_membership VALUES (1);
SELECT x, x NOT IN materialize('256') FROM squality_check_string_membership;
DROP TABLE squality_check_string_membership;

DROP TABLE IF EXISTS squality_check_date_membership;
CREATE TABLE squality_check_date_membership
(
    x Date,
    y UInt32,
    CONSTRAINT c CHECK x IN (y, NULL)
) ENGINE = Memory;
INSERT INTO squality_check_date_membership VALUES ('1970-01-01', 65536); -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_check_date_membership;

DROP TABLE IF EXISTS squality_check_decimal_membership;
CREATE TABLE squality_check_decimal_membership
(
    x Decimal128(0),
    y Float64,
    CONSTRAINT c CHECK x IN materialize(y)
) ENGINE = Memory;
INSERT INTO squality_check_decimal_membership VALUES ('9007199254740993', 9007199254740992); -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_check_decimal_membership;

DROP TABLE IF EXISTS squality_check_datetime_membership;
CREATE TABLE squality_check_datetime_membership
(
    x Date,
    y DateTime,
    CONSTRAINT c CHECK x NOT IN materialize(y)
) ENGINE = Memory;
INSERT INTO squality_check_datetime_membership VALUES ('2000-01-01', '2000-01-01 12:00:00'); -- { serverError VIOLATED_CONSTRAINT }
DROP TABLE squality_check_datetime_membership;

DROP TABLE IF EXISTS squality_check_datetime_with_null;
CREATE TABLE squality_check_datetime_with_null
(
    x Date,
    y DateTime,
    CONSTRAINT c CHECK x IN (y, NULL)
) ENGINE = Memory;
INSERT INTO squality_check_datetime_with_null VALUES ('2000-01-01', '2000-01-01 12:00:00');
SELECT x, x IN (y, NULL) FROM squality_check_datetime_with_null;
DROP TABLE squality_check_datetime_with_null;
