-- A normal projection serving the read must keep the header of the steps it replaces: a column read only to count rows
-- must not be passed through to the next step.
SET optimize_use_projections = 1, force_optimize_projection = 1;
SET query_plan_remove_unused_columns = 0, query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS t_proj_unused_column;
CREATE TABLE t_proj_unused_column (a UInt64, b UInt64, PROJECTION p (SELECT * ORDER BY b)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_proj_unused_column VALUES (1, 10), (2, 20);

SELECT count() OVER () FROM t_proj_unused_column;
SELECT count() FROM (SELECT toUInt64(7) AS a FROM t_proj_unused_column) AS x, (SELECT 1 AS b) AS y;
SELECT count() FROM (SELECT NULL FROM remote('127.0.0.{1,2}', currentDatabase(), t_proj_unused_column) SHUFFLE LIMIT 100)
SETTINGS allow_experimental_shuffle_query = 1;

-- Some parts have no projection.
DROP TABLE IF EXISTS t_proj_unused_column_partial;
CREATE TABLE t_proj_unused_column_partial (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_proj_unused_column_partial VALUES (1, 10);
ALTER TABLE t_proj_unused_column_partial ADD PROJECTION p (SELECT * ORDER BY b);
INSERT INTO t_proj_unused_column_partial VALUES (2, 20);
SELECT count() OVER () FROM t_proj_unused_column_partial;

DROP TABLE t_proj_unused_column;
DROP TABLE t_proj_unused_column_partial;
