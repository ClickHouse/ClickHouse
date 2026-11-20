-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_sort_nan;
CREATE TABLE t_substrait_sort_nan (k Int64, f Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_sort_nan VALUES (1, nan);

SELECT countIf(explain LIKE '%is_nan:fp64%SORT_DIRECTION_ASC_NULLS_LAST%SORT_DIRECTION_DESC_NULLS_LAST%') FROM (EXPLAIN SUBSTRAIT SELECT f FROM t_substrait_sort_nan ORDER BY f DESC);
SELECT countIf(explain LIKE '%is_nan:fp64%SORT_DIRECTION_DESC_NULLS_FIRST%SORT_DIRECTION_ASC_NULLS_FIRST%') FROM (EXPLAIN SUBSTRAIT SELECT f FROM t_substrait_sort_nan ORDER BY f NULLS FIRST);
SELECT countIf(explain LIKE '%is_nan%') FROM (EXPLAIN SUBSTRAIT SELECT k FROM t_substrait_sort_nan ORDER BY k DESC);

DROP TABLE t_substrait_sort_nan;
