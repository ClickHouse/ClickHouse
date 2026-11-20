-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_float_join_key;
CREATE TABLE t_substrait_float_join_key (k Int64, f Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_float_join_key VALUES (1, 1);

EXPLAIN SUBSTRAIT SELECT a.k FROM t_substrait_float_join_key AS a JOIN t_substrait_float_join_key AS b ON a.f = b.f; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT a.k FROM t_substrait_float_join_key AS a JOIN t_substrait_float_join_key AS b ON a.f = b.f OR a.k = b.k + 1; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_float_join_key;
