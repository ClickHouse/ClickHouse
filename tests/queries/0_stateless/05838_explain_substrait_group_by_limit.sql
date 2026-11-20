-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_group_by_limit;
CREATE TABLE t_substrait_group_by_limit (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_group_by_limit VALUES (1);

SELECT count() FROM (EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_group_by_limit GROUP BY k LIMIT 5);

DROP TABLE t_substrait_group_by_limit;
