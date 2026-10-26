-- A function-form carrier (`length(s)`) over a column used by the `WHERE` of a filtered projection
-- must not be lowered to a subcolumn read in the subquery: the filter of the query must imply the
-- `WHERE` of the projection, which is matched literally.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;

DROP TABLE IF EXISTS t_push_proj_where;
CREATE TABLE t_push_proj_where
(
    id UInt64,
    s String,
    PROJECTION p (SELECT id, s WHERE length(s) > 98 ORDER BY id)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128;
INSERT INTO t_push_proj_where SELECT number, repeat('a', number % 100) FROM numbers(10000);

SELECT count() FROM (SELECT s FROM t_push_proj_where) WHERE length(s) > 98
SETTINGS force_optimize_projection = 1;

DROP TABLE t_push_proj_where;
