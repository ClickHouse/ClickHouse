DROP TABLE IF EXISTS t_memory_alias_dependency;
CREATE TABLE t_memory_alias_dependency (a UInt64, x UInt64) ENGINE = Memory;
INSERT INTO t_memory_alias_dependency VALUES (1, 2);
ALTER TABLE t_memory_alias_dependency
    DROP COLUMN a,
    ADD COLUMN IF NOT EXISTS a UInt64 DEFAULT rand64(),
    ADD COLUMN z UInt64 ALIAS a + 100,
    DROP COLUMN x,
    ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT z FIRST;
SELECT x = a + 100 FROM t_memory_alias_dependency;
DROP TABLE t_memory_alias_dependency;

DROP TABLE IF EXISTS t_memory_tuple_alias_dependency;
CREATE TABLE t_memory_tuple_alias_dependency (a Tuple(v UInt64), x UInt64) ENGINE = Memory;
INSERT INTO t_memory_tuple_alias_dependency VALUES ((7), 2);
ALTER TABLE t_memory_tuple_alias_dependency
    DROP COLUMN a,
    ADD COLUMN IF NOT EXISTS a Tuple(v UInt64) DEFAULT tuple(1),
    ADD COLUMN z Tuple(v UInt64) ALIAS tuple(a.v + 100),
    DROP COLUMN x,
    ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT z.v;
SELECT x FROM t_memory_tuple_alias_dependency;
DROP TABLE t_memory_tuple_alias_dependency;

SET flatten_nested = 0;
DROP TABLE IF EXISTS t_memory_unflattened_nested;
CREATE TABLE t_memory_unflattened_nested (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_unflattened_nested VALUES (7);
ALTER TABLE t_memory_unflattened_nested
    DROP COLUMN x,
    ADD COLUMN IF NOT EXISTS n Nested(a UInt64) DEFAULT [tuple(257)];
SELECT n FROM t_memory_unflattened_nested;
DROP TABLE t_memory_unflattened_nested;

DROP TABLE IF EXISTS t_memory_alias_target;
DROP TABLE IF EXISTS t_memory_alias;
CREATE TABLE t_memory_alias_target (x UInt8) ENGINE = Memory;
CREATE TABLE t_memory_alias ENGINE = Alias('t_memory_alias_target');
INSERT INTO t_memory_alias_target VALUES (7);
ALTER TABLE t_memory_alias DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT 9;
SELECT x FROM t_memory_alias;
DROP TABLE t_memory_alias;
DROP TABLE t_memory_alias_target;
