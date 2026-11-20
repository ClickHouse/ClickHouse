-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_join_sort_limits;
CREATE TABLE t_substrait_join_sort_limits (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_join_sort_limits VALUES (1);

EXPLAIN SUBSTRAIT SELECT a.k FROM t_substrait_join_sort_limits AS a JOIN t_substrait_join_sort_limits AS b ON a.k = b.k SETTINGS max_rows_to_sort = 1, sort_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_join_sort_limits;
