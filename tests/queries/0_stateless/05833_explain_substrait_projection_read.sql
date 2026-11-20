-- Tags: no-fasttest
-- no-fasttest: Substrait is not built in fast test

SET enable_substrait = 1;

DROP TABLE IF EXISTS t_substrait_projection_read;
CREATE TABLE t_substrait_projection_read (k Int64, PROJECTION p (SELECT k, count() GROUP BY k)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_substrait_projection_read VALUES (1), (1);

EXPLAIN SUBSTRAIT SELECT k FROM mergeTreeProjection(currentDatabase(), t_substrait_projection_read, p); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_substrait_projection_read;
