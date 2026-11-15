-- This test checks the behavior of adding new Decimal columns to a table with existing data.
-- It verifies that for old rows, the new column is filled with the default value for the type (0),
-- unless a specific DEFAULT expression is provided.

DROP TABLE IF EXISTS test_alter_add_decimal;
CREATE TABLE test_alter_add_decimal (id UInt32) ENGINE = Memory;
INSERT INTO test_alter_add_decimal VALUES (1), (2);

-- Test Decimal512
SELECT '--- Stage 1: Before adding Decimal512 ---' AS stage;
SELECT * FROM test_alter_add_decimal ORDER BY id;
SHOW CREATE TABLE test_alter_add_decimal;

ALTER TABLE test_alter_add_decimal ADD COLUMN d512 Decimal512(10);
SELECT '--- Stage 2: After adding Decimal512 (no default) ---' AS stage;
SELECT * FROM test_alter_add_decimal ORDER BY id;
SHOW CREATE TABLE test_alter_add_decimal;

-- Test Decimal256
ALTER TABLE test_alter_add_decimal ADD COLUMN d256 Decimal256(8);
SELECT '--- Stage 3: After adding Decimal256 (no default) ---' AS stage;
SELECT * FROM test_alter_add_decimal ORDER BY id;
SHOW CREATE TABLE test_alter_add_decimal;

-- Test Decimal512 with a DEFAULT clause for comparison
ALTER TABLE test_alter_add_decimal ADD COLUMN d512_default Decimal512(10) DEFAULT toDecimal512('99.99', 10);
SELECT '--- Stage 4: After adding Decimal512 with DEFAULT ---' AS stage;
SELECT * FROM test_alter_add_decimal ORDER BY id;
SHOW CREATE TABLE test_alter_add_decimal;

DROP TABLE IF EXISTS test_alter_add_decimal;
