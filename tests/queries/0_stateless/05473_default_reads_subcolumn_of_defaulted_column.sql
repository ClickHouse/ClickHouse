-- A DEFAULT that reads another column with a DEFAULT through a subcolumn, or through an ALIAS, MATERIALIZED or EPHEMERAL
-- column, sees the inserted value of that column in the rows that supply it.

DROP TABLE IF EXISTS t_subcolumns;
DROP TABLE IF EXISTS t_through;
DROP TABLE IF EXISTS t_chain;
DROP TABLE IF EXISTS t_cycle;

CREATE TABLE t_subcolumns
(
    id UInt32,
    m Map(String, UInt32) DEFAULT map('x', 7),
    n Nullable(UInt32) DEFAULT NULL,
    j JSON DEFAULT '{"v" : 7}',
    d_m Array(String) DEFAULT m.keys,
    d_n UInt8 DEFAULT n.null,
    d_j UInt32 DEFAULT j.v::UInt32
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_subcolumns FORMAT JSONEachRow {"id" : 1}, {"id" : 2, "m" : {"y" : 5}, "n" : 5, "j" : {"v" : 5}};

INSERT INTO t_subcolumns VALUES (3, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT), (4, map('y', 5), 5, '{"v" : 5}', DEFAULT, DEFAULT, DEFAULT);
INSERT INTO t_subcolumns SETTINGS async_insert = 1, wait_for_async_insert = 1 FORMAT JSONEachRow {"id" : 5}, {"id" : 6, "m" : {"y" : 5}, "n" : 5, "j" : {"v" : 5}};

SELECT id, d_m, d_n, d_j FROM t_subcolumns ORDER BY id;

CREATE TABLE t_through
(
    id UInt32,
    p UInt32 DEFAULT 7,
    tup Tuple(v UInt32) DEFAULT tuple(7),
    e_alias UInt32 ALIAS p,
    e_mat UInt32 MATERIALIZED p,
    e_eph UInt32 EPHEMERAL p,
    e_sub UInt32 ALIAS tup.v,
    d_alias UInt32 DEFAULT e_alias,
    d_mat UInt32 DEFAULT e_mat,
    d_eph UInt32 DEFAULT e_eph,
    d_sub UInt32 DEFAULT e_sub
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_through FORMAT JSONEachRow {"id" : 1}, {"id" : 2, "p" : 5, "tup" : {"v" : 5}};

SELECT id, d_alias, d_mat, d_eph, d_sub FROM t_through ORDER BY id;

CREATE TABLE t_chain (id UInt32, x UInt32 DEFAULT 7, tup Tuple(v UInt32) DEFAULT tuple(x), d UInt32 DEFAULT tup.v) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_chain FORMAT JSONEachRow {"id" : 1}, {"id" : 2, "x" : 5}, {"id" : 3, "tup" : {"v" : 4}};

SELECT id, x, tup.v, d FROM t_chain ORDER BY id;

-- Defaults that read each other through a subcolumn cannot be ordered, as for `INSERT INTO t_cycle (id) VALUES`.
CREATE TABLE t_cycle (id UInt32, a Tuple(x UInt32) DEFAULT tuple(b), b UInt32 DEFAULT a.x) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_cycle -- { error UNKNOWN_IDENTIFIER }
FORMAT JSONEachRow {"id" : 1, "a" : {"x" : 5}}, {"id" : 2, "b" : 4};

DROP TABLE t_subcolumns;
DROP TABLE t_through;
DROP TABLE t_chain;
DROP TABLE t_cycle;
