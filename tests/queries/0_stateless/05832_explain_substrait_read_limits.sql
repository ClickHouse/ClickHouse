-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_read_limits;
CREATE TABLE t_substrait_read_limits (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_read_limits VALUES (1), (2);

EXPLAIN SUBSTRAIT SELECT sum(k) FROM t_substrait_read_limits SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT sum(k) FROM (SELECT k FROM t_substrait_read_limits SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break'); -- { serverError NOT_IMPLEMENTED }
EXPLAIN SUBSTRAIT SELECT sum(k) FROM t_substrait_read_limits SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_read_limits;
