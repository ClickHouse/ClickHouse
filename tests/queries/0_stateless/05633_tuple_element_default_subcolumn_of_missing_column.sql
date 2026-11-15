-- A default that references a subcolumn of another column with a dotted name (`c.n.x` of the column `c.n`)
-- is evaluated when that column is missing as well and is computed by its own default.

DROP TABLE IF EXISTS t_subcolumn_of_missing_column;

CREATE TABLE t_subcolumn_of_missing_column
(
    id UInt8,
    `c.n` Tuple(x UInt8) DEFAULT tuple(7),
    `c.a` Array(UInt8) DEFAULT [1, 2, 3],
    c Tuple(b UInt8 DEFAULT `c.n.x`, s UInt64 DEFAULT c.a.size0)
) ENGINE = Memory;

INSERT INTO t_subcolumn_of_missing_column (id) VALUES (1);
INSERT INTO t_subcolumn_of_missing_column (id, `c.n`) VALUES (2, tuple(9));
SELECT id, `c.n`, `c.a`, c FROM t_subcolumn_of_missing_column ORDER BY id;

-- The same with a column-level default, and when reading parts written before the columns were added.
DROP TABLE t_subcolumn_of_missing_column;
CREATE TABLE t_subcolumn_of_missing_column (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_subcolumn_of_missing_column VALUES (1);
ALTER TABLE t_subcolumn_of_missing_column
    ADD COLUMN `c.n` Tuple(x UInt8) DEFAULT tuple(id + 5),
    ADD COLUMN d UInt8 DEFAULT `c.n.x` + 1;
SELECT id, d FROM t_subcolumn_of_missing_column ORDER BY id;
SELECT id, `c.n`, d FROM t_subcolumn_of_missing_column ORDER BY id;
INSERT INTO t_subcolumn_of_missing_column (id) VALUES (2);
SELECT id, `c.n`, d FROM t_subcolumn_of_missing_column ORDER BY id;

DROP TABLE t_subcolumn_of_missing_column;
