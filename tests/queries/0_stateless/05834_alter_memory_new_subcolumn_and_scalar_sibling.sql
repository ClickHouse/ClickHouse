DROP TABLE IF EXISTS t_memory_new_nullable_subcolumn;
CREATE TABLE t_memory_new_nullable_subcolumn (a UInt8, x UInt64) ENGINE = Memory;
INSERT INTO t_memory_new_nullable_subcolumn VALUES (7, 0);
ALTER TABLE t_memory_new_nullable_subcolumn MODIFY COLUMN a Nullable(UInt8);
ALTER TABLE t_memory_new_nullable_subcolumn DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a.null + 1;
SELECT x FROM t_memory_new_nullable_subcolumn;
DROP TABLE t_memory_new_nullable_subcolumn;

DROP TABLE IF EXISTS t_memory_new_tuple_subcolumn;
CREATE TABLE t_memory_new_tuple_subcolumn (a Tuple(v UInt8), x UInt64) ENGINE = Memory;
INSERT INTO t_memory_new_tuple_subcolumn VALUES ((7), 0);
ALTER TABLE t_memory_new_tuple_subcolumn MODIFY COLUMN a Tuple(v UInt8, w UInt8);
ALTER TABLE t_memory_new_tuple_subcolumn DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a.w + 1;
SELECT x FROM t_memory_new_tuple_subcolumn;
DROP TABLE t_memory_new_tuple_subcolumn;

DROP TABLE IF EXISTS t_memory_scalar_sibling;
CREATE TABLE t_memory_scalar_sibling (id UInt8, `n.a` Array(UInt8), x UInt64) ENGINE = Memory;
INSERT INTO t_memory_scalar_sibling VALUES (1, [7, 8], 0);
ALTER TABLE t_memory_scalar_sibling ADD COLUMN `n.b` UInt8;
ALTER TABLE t_memory_scalar_sibling MODIFY COLUMN x DEFAULT `n.b` + 1;
ALTER TABLE t_memory_scalar_sibling CLEAR COLUMN x;
SELECT x FROM t_memory_scalar_sibling;
ALTER TABLE t_memory_scalar_sibling DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT `n.b` + 1;
SELECT x FROM t_memory_scalar_sibling;
DROP TABLE t_memory_scalar_sibling;
