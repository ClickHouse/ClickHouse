DROP TABLE IF EXISTS test_decimal_256_to_512;
CREATE TABLE test_decimal_256_to_512 (d Decimal256(6)) ENGINE = Memory;
INSERT INTO test_decimal_256_to_512 VALUES (toDecimal256('12345.6789', 6));
SELECT d, toTypeName(d) FROM test_decimal_256_to_512;
ALTER TABLE test_decimal_256_to_512 MODIFY COLUMN d Decimal512(6);
SELECT d, toTypeName(d) FROM test_decimal_256_to_512;
DROP TABLE IF EXISTS test_decimal_256_to_512;
