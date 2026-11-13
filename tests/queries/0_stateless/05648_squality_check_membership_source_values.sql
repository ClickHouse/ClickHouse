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
