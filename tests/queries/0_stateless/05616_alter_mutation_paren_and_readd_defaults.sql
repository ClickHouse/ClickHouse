-- A quoted parenthesis must not glue the next persisted mutation command onto this one.
DROP TABLE IF EXISTS t_mutation_paren;
CREATE TABLE t_mutation_paren (id UInt8, x String, y UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mutation_paren VALUES (1, 'a', 3);
SYSTEM STOP MERGES t_mutation_paren;
ALTER TABLE t_mutation_paren UPDATE x = '(' WHERE id = 1, UPDATE y = 2 WHERE id = 1 SETTINGS alter_sync = 0, mutations_sync = 0;
DETACH TABLE t_mutation_paren;
ATTACH TABLE t_mutation_paren;
SYSTEM START MERGES t_mutation_paren;
ALTER TABLE t_mutation_paren UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT x, y FROM t_mutation_paren ORDER BY id;
DROP TABLE t_mutation_paren;

-- An exact-column command in the same text keeps its own mark.
DROP TABLE IF EXISTS t_mutation_exact_paren;
CREATE TABLE t_mutation_exact_paren (id UInt8, x String, y UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mutation_exact_paren VALUES (1, 'a', 3);
SYSTEM STOP MERGES t_mutation_exact_paren;
ALTER TABLE t_mutation_exact_paren UPDATE x = '(' WHERE id = 1, DROP COLUMN y, ADD COLUMN IF NOT EXISTS y UInt8 DEFAULT 4 SETTINGS alter_sync = 0, mutations_sync = 0;
DETACH TABLE t_mutation_exact_paren;
ATTACH TABLE t_mutation_exact_paren;
SYSTEM START MERGES t_mutation_exact_paren;
ALTER TABLE t_mutation_exact_paren UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT x, y FROM t_mutation_exact_paren ORDER BY id;
DROP TABLE t_mutation_exact_paren;

-- Dropping one Nested column twice must leave the sibling array in place.
DROP TABLE IF EXISTS t_nested_repeat_drop;
CREATE TABLE t_nested_repeat_drop (id UInt8, n Nested(a UInt8, c UInt8))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_nested_repeat_drop VALUES (1, [7, 8], [9, 10]);
ALTER TABLE t_nested_repeat_drop
    DROP COLUMN `n.a`, ADD COLUMN IF NOT EXISTS `n.a` Array(UInt8),
    DROP COLUMN `n.a`, ADD COLUMN IF NOT EXISTS `n.a` Array(UInt8)
    SETTINGS mutations_sync = 2;
SELECT n.c FROM t_nested_repeat_drop;
DROP TABLE t_nested_repeat_drop;

-- The recreated default sees `a` as UInt8. The stored `a` stays UInt64.
DROP TABLE IF EXISTS t_memory_default_type;
CREATE TABLE t_memory_default_type (a UInt64, x UInt64) ENGINE = Memory;
INSERT INTO t_memory_default_type VALUES (257, 7);
ALTER TABLE t_memory_default_type MODIFY COLUMN a UInt8;
SELECT a, a + 1 FROM t_memory_default_type;
ALTER TABLE t_memory_default_type DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a + 1;
SELECT a, x FROM t_memory_default_type;
ALTER TABLE t_memory_default_type MODIFY COLUMN a UInt64;
SELECT a, x FROM t_memory_default_type;
DROP TABLE t_memory_default_type;
