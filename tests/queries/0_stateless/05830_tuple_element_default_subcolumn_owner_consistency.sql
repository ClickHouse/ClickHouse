-- A subcolumn referenced in a default expression may belong to several columns: `x.n.v` is a subcolumn of both
-- `x Tuple(n Tuple(v UInt8))` and `x.n Tuple(v UInt8)`. The column it is read from must not depend on the set of
-- columns supplied by the insert, nor on the order of the columns in the table: it is always the column with
-- the shortest name that has such a subcolumn, as the analyzer resolves identifiers.

DROP TABLE IF EXISTS t_subcolumn_owner;

CREATE TABLE t_subcolumn_owner
(
    id UInt8,
    `x.n` Tuple(v UInt8) DEFAULT tuple(99),
    x Tuple(n Tuple(v UInt8)) DEFAULT tuple(tuple(7)),
    d UInt8 DEFAULT x.n.v
) ENGINE = Memory;

INSERT INTO t_subcolumn_owner (id, x) VALUES (1, tuple(tuple(7)));
INSERT INTO t_subcolumn_owner (id, x, `x.n`) VALUES (2, tuple(tuple(7)), tuple(99));
INSERT INTO t_subcolumn_owner (id, `x.n`) VALUES (3, tuple(99));
INSERT INTO t_subcolumn_owner (id) VALUES (4);
SELECT id, x, `x.n`, d FROM t_subcolumn_owner ORDER BY id;

-- The same with the other order of the columns in the table.
DROP TABLE t_subcolumn_owner;

CREATE TABLE t_subcolumn_owner
(
    id UInt8,
    x Tuple(n Tuple(v UInt8)) DEFAULT tuple(tuple(7)),
    `x.n` Tuple(v UInt8) DEFAULT tuple(99),
    d UInt8 DEFAULT x.n.v
) ENGINE = Memory;

INSERT INTO t_subcolumn_owner (id, x) VALUES (1, tuple(tuple(7)));
INSERT INTO t_subcolumn_owner (id, x, `x.n`) VALUES (2, tuple(tuple(7)), tuple(99));
INSERT INTO t_subcolumn_owner (id, `x.n`) VALUES (3, tuple(99));
INSERT INTO t_subcolumn_owner (id) VALUES (4);
SELECT id, x, `x.n`, d FROM t_subcolumn_owner ORDER BY id;

-- When the column with the shorter name has no such subcolumn, the reference belongs to the dotted-name column.
DROP TABLE t_subcolumn_owner;

CREATE TABLE t_subcolumn_owner
(
    id UInt8,
    `x.n` Tuple(v UInt8) DEFAULT tuple(99),
    x Tuple(m UInt8) DEFAULT tuple(7),
    d UInt8 DEFAULT x.n.v
) ENGINE = Memory;

INSERT INTO t_subcolumn_owner (id, x) VALUES (1, tuple(5));
INSERT INTO t_subcolumn_owner (id, x, `x.n`) VALUES (2, tuple(5), tuple(98));
INSERT INTO t_subcolumn_owner (id, `x.n`) VALUES (3, tuple(98));
INSERT INTO t_subcolumn_owner (id) VALUES (4);
SELECT id, x, `x.n`, d FROM t_subcolumn_owner ORDER BY id;

DROP TABLE t_subcolumn_owner;
