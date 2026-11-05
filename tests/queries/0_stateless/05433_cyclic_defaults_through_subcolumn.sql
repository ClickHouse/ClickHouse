SET enable_json_type = 1;
DROP TABLE IF EXISTS t_cyc;
DROP TABLE IF EXISTS t_ok;
DROP TABLE IF EXISTS t_ok_nested;

-- A cycle through a subcolumn was invisible to the name-based check and recursed at query time.
CREATE TABLE t_cyc (line String, j Tuple(a Int64) ALIAS tuple(x), x Int64 ALIAS j.a) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t_cyc (line String, j Tuple(a Int64, b Int64) EPHEMERAL (x, x), x Int64 MATERIALIZED j.a) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t_cyc (line String, j Tuple(a Int64, b Int64) DEFAULT (x, x), x Int64 DEFAULT j.a + y, y Int64 DEFAULT j.b) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t_cyc (line String, js JSON EPHEMERAL CAST(concat('{"a":', toString(x), '}'), 'JSON'), x Int64 MATERIALIZED js.a::Int64) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

-- The first part of a compound identifier is a dependency only when its type has that subcolumn.
CREATE TABLE t_cyc (j Int64 DEFAULT x, x Int64 DEFAULT j.a) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

-- Not cycles.
CREATE TABLE t_ok (line String, j Tuple(a Int64, b Int64) EPHEMERAL JSONExtract(line, 'Tuple(a Int64, b Int64)'), x Int64 MATERIALIZED j.a, y Int64 MATERIALIZED j.b + x) ENGINE = Memory;
CREATE TABLE t_ok_nested (line String, n Nested(a Int64, b Int64), x Array(Int64) MATERIALIZED n.a) ENGINE = Memory;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('t_ok', 't_ok_nested');

DROP TABLE t_ok;
DROP TABLE t_ok_nested;
