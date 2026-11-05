-- Tags: no-ordinary-database, no-replicated-database
-- Tag no-ordinary-database: the ATTACH spelling below needs a table UUID
-- Tag no-replicated-database: explicit table UUIDs are rejected there by default

-- A full-definition ATTACH skips the validation of defaults that CREATE runs, like a table
-- created by an older server: the analyzer must refuse the cycle instead of recursing.
DROP TABLE IF EXISTS t_alias_cycle_plain SYNC;
DROP TABLE IF EXISTS t_alias_cycle_sub SYNC;
DROP TABLE IF EXISTS t_alias_chain SYNC;

ATTACH TABLE t_alias_cycle_plain UUID '7d4b2c6e-1a7f-4b1e-9c3d-0a1b2c3d4e01' (k Int64, a Int64 ALIAS b, b Int64 ALIAS a) ENGINE = Memory;
INSERT INTO t_alias_cycle_plain (k) VALUES (1);
SELECT a FROM t_alias_cycle_plain; -- { serverError CYCLIC_ALIASES }

ATTACH TABLE t_alias_cycle_sub UUID '7d4b2c6e-1a7f-4b1e-9c3d-0a1b2c3d4e02' (line String, j Tuple(a Int64) ALIAS tuple(x), x Int64 ALIAS j.a) ENGINE = Memory;
INSERT INTO t_alias_cycle_sub (line) VALUES ('q');
SELECT x FROM t_alias_cycle_sub; -- { serverError CYCLIC_ALIASES }
SELECT j.a FROM t_alias_cycle_sub; -- { serverError CYCLIC_ALIASES }

-- Not a cycle: two ALIAS columns read a third one, also through a subcolumn.
CREATE TABLE t_alias_chain (line String, j Tuple(a Int64, b Int64) ALIAS JSONExtract(line, 'Tuple(a Int64, b Int64)'), x Int64 ALIAS j.a, y Int64 ALIAS j.b + x) ENGINE = Memory;
INSERT INTO t_alias_chain (line) VALUES ('{"a":1,"b":2}');
SELECT x, y FROM t_alias_chain;
SELECT * FROM t_alias_chain SETTINGS asterisk_include_alias_columns = 1;

DROP TABLE t_alias_cycle_plain SYNC;
DROP TABLE t_alias_cycle_sub SYNC;
DROP TABLE t_alias_chain SYNC;
