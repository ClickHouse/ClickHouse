-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_subcolumns;
CREATE TABLE t_substrait_subcolumns (k Int64, v Nullable(Int64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_subcolumns VALUES (1, NULL);

EXPLAIN SUBSTRAIT SELECT v.null FROM t_substrait_subcolumns; -- { serverError NOT_IMPLEMENTED }
SELECT countIf(explain LIKE '%is_null%'), countIf(explain LIKE '%v.null%') FROM (EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_subcolumns WHERE v IS NULL);

DROP TABLE t_substrait_subcolumns;
