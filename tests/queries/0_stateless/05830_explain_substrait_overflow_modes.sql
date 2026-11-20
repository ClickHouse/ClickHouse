-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_overflow;
CREATE TABLE t_substrait_overflow (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_overflow VALUES (1), (2);

EXPLAIN SUBSTRAIT SELECT k, count() FROM t_substrait_overflow GROUP BY k SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any'; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT DISTINCT k FROM t_substrait_overflow SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_overflow ORDER BY k SETTINGS max_rows_to_sort = 1, sort_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT a.k FROM t_substrait_overflow AS a JOIN t_substrait_overflow AS b ON a.k = b.k SETTINGS max_rows_in_join = 1, join_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_overflow;
