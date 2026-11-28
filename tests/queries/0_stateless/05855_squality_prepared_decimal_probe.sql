SET in_null_as_unknown = 1;

SELECT
    toDecimal32('1', 0) IN (SELECT toFloat64(1)),
    toDecimal32('1', 0) IN materialize(toFloat64(1)),
    toDecimal32('1', 0) NOT IN (SELECT toFloat64(1)),
    toDecimal32('1', 0) IN (SELECT toUInt8(1));

SELECT toDecimal128('9007199254740993', 0) IN
    (SELECT toFloat64(9007199254740992));

DROP TABLE IF EXISTS squality_prepared_decimal_probe_set;
CREATE TABLE squality_prepared_decimal_probe_set (k Float64) ENGINE = Set;
INSERT INTO squality_prepared_decimal_probe_set VALUES (1);
SELECT toDecimal32('1', 0) IN squality_prepared_decimal_probe_set;
DROP TABLE squality_prepared_decimal_probe_set;
