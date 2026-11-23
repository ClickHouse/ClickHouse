-- Tags: no-parallel
-- ^ creates user defined functions, which are global.

-- a projection keeps the body of an SQL function, so replacing the function does not change it

DROP TABLE IF EXISTS t_projection_sql_function;
DROP TABLE IF EXISTS t_projection_sql_function_limits;
DROP FUNCTION IF EXISTS projection_sql_function_positive;
DROP FUNCTION IF EXISTS projection_sql_function_threshold;

CREATE TABLE t_projection_sql_function_limits (v Int32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_projection_sql_function_limits VALUES (10);

CREATE FUNCTION projection_sql_function_positive AS x -> x > 0;
CREATE FUNCTION projection_sql_function_threshold AS () -> (SELECT max(v) FROM t_projection_sql_function_limits);

CREATE TABLE t_projection_sql_function
(
    a Int32,
    PROJECTION p_created (SELECT a WHERE projection_sql_function_positive(a) ORDER BY a)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_sql_function ADD PROJECTION p_added (SELECT a WHERE projection_sql_function_positive(a) ORDER BY a);
-- the scalar subquery in the function is frozen at its current value
ALTER TABLE t_projection_sql_function ADD PROJECTION p_scalar (SELECT a WHERE a > projection_sql_function_threshold() ORDER BY a);

CREATE OR REPLACE FUNCTION projection_sql_function_positive AS x -> x > 10;
INSERT INTO t_projection_sql_function_limits VALUES (100);

INSERT INTO t_projection_sql_function VALUES (5), (20);

SELECT 'p_created', a FROM mergeTreeProjection(currentDatabase(), 't_projection_sql_function', 'p_created') ORDER BY a;
SELECT 'p_added', a FROM mergeTreeProjection(currentDatabase(), 't_projection_sql_function', 'p_added') ORDER BY a;
SELECT 'p_scalar', a FROM mergeTreeProjection(currentDatabase(), 't_projection_sql_function', 'p_scalar') ORDER BY a;

SELECT a FROM t_projection_sql_function WHERE a > 0 ORDER BY a SETTINGS optimize_use_projections = 1;
SELECT a FROM t_projection_sql_function WHERE a > 0 ORDER BY a SETTINGS optimize_use_projections = 0;

DROP TABLE t_projection_sql_function;
DROP TABLE t_projection_sql_function_limits;
DROP FUNCTION projection_sql_function_positive;
DROP FUNCTION projection_sql_function_threshold;
