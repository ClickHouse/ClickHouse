-- Tags: no-ordinary-database, no-replicated-database
-- Tag no-ordinary-database: the ATTACH spelling below needs a table UUID
-- Tag no-replicated-database: explicit table UUIDs are rejected there by default

DROP TABLE IF EXISTS t_cast SYNC;
DROP TABLE IF EXISTS t_mat_parent SYNC;
DROP TABLE IF EXISTS t_null_default SYNC;
DROP TABLE IF EXISTS t_eph_given SYNC;
DROP TABLE IF EXISTS t_eph_plain SYNC;
DROP TABLE IF EXISTS t_insert_cycle SYNC;

-- The expression of the parent has another type than the parent: the declared type wins.
CREATE TABLE t_cast (line String, j Tuple(a Int64, b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Int32, b Int32)'), x Int64 MATERIALIZED j.a) ENGINE = Memory;
INSERT INTO t_cast (line) VALUES ('{"a":1,"b":2}');
SELECT x, toTypeName(x) FROM t_cast;

-- The parent's expression reads a MATERIALIZED column that is not in the block either.
CREATE TABLE t_mat_parent (line String, m Int64 MATERIALIZED length(line), j Tuple(a Int64, b Int64) EPHEMERAL (m, m), x Int64 MATERIALIZED j.a) ENGINE = Memory;
INSERT INTO t_mat_parent (line) VALUES ('abc');
SELECT m, x FROM t_mat_parent;

-- A Nullable input converted to the default is untouched by the change.
CREATE TABLE t_null_default (a Int64, x Int64 DEFAULT a + 1) ENGINE = Memory;
INSERT INTO t_null_default (a) SETTINGS insert_null_as_default = 1 SELECT CAST(NULL, 'Nullable(Int64)');
SELECT a, x FROM t_null_default;

-- An EPHEMERAL column given in the INSERT keeps the given value.
CREATE TABLE t_eph_given (line String, j Tuple(a Int64, b Int64) EPHEMERAL, x Int64 MATERIALIZED j.a) ENGINE = Memory;
INSERT INTO t_eph_given (line, j) VALUES ('', (9, 8));
SELECT x FROM t_eph_given;

-- An EPHEMERAL column without an expression is its type default.
CREATE TABLE t_eph_plain (line String, j Tuple(a Int64, b Int64) EPHEMERAL, x Int64 MATERIALIZED j.a) ENGINE = Memory;
INSERT INTO t_eph_plain (line) VALUES ('');
SELECT x FROM t_eph_plain;

-- A cycle that an older server accepted is refused by the analyzer on INSERT.
ATTACH TABLE t_insert_cycle UUID '7d4b2c6e-1a7f-4b1e-9c3d-0a1b2c3d4e03' (line String, j Tuple(a Int64, b Int64) EPHEMERAL (x, x), x Int64 MATERIALIZED j.a) ENGINE = Memory;
INSERT INTO t_insert_cycle (line) VALUES ('q'); -- { serverError CYCLIC_ALIASES }

DROP TABLE t_cast SYNC;
DROP TABLE t_mat_parent SYNC;
DROP TABLE t_null_default SYNC;
DROP TABLE t_eph_given SYNC;
DROP TABLE t_eph_plain SYNC;
DROP TABLE t_insert_cycle SYNC;
