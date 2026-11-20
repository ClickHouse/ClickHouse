-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_modulo;
CREATE TABLE t_substrait_modulo (x Int64, y Int64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_substrait_modulo VALUES (7, 3);

EXPLAIN SUBSTRAIT SELECT x % y FROM t_substrait_modulo; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT x % -1 FROM t_substrait_modulo; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_modulo;
