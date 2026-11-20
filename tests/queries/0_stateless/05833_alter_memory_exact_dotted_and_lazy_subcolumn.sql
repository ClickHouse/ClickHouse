DROP TABLE IF EXISTS t_memory_exact_dotted;
CREATE TABLE t_memory_exact_dotted (a Tuple(v UInt8), `a.v` UInt8) ENGINE = Memory;
INSERT INTO t_memory_exact_dotted VALUES ((7), 9);
ALTER TABLE t_memory_exact_dotted CLEAR COLUMN `a.v`;
SELECT `a.v` FROM t_memory_exact_dotted;
ALTER TABLE t_memory_exact_dotted ADD COLUMN x UInt8 DEFAULT 0;
ALTER TABLE t_memory_exact_dotted DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT `a.v` + 1;
SELECT x FROM t_memory_exact_dotted;
DROP TABLE t_memory_exact_dotted;

DROP TABLE IF EXISTS t_memory_exact_dotted_alias;
CREATE TABLE t_memory_exact_dotted_alias (a Tuple(v UInt8), `a.v` UInt8 ALIAS 9, x UInt8) ENGINE = Memory;
INSERT INTO t_memory_exact_dotted_alias VALUES ((7), 1);
ALTER TABLE t_memory_exact_dotted_alias DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT `a.v` + 1;
SELECT x FROM t_memory_exact_dotted_alias;
DROP TABLE t_memory_exact_dotted_alias;

DROP TABLE IF EXISTS t_memory_lazy_subcolumn;
CREATE TABLE t_memory_lazy_subcolumn (a Tuple(v String, w UInt8), x UInt64 DEFAULT a.w + 1) ENGINE = Memory;
INSERT INTO t_memory_lazy_subcolumn VALUES (('bad', 7), 0);
ALTER TABLE t_memory_lazy_subcolumn MODIFY COLUMN a Tuple(v UInt8, w UInt8);
SELECT a.w, a.w + 1 FROM t_memory_lazy_subcolumn;
ALTER TABLE t_memory_lazy_subcolumn CLEAR COLUMN x;
SELECT x FROM t_memory_lazy_subcolumn;
ALTER TABLE t_memory_lazy_subcolumn DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a.w + 1;
SELECT x FROM t_memory_lazy_subcolumn;
DROP TABLE t_memory_lazy_subcolumn;
