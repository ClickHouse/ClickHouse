-- Pushing a subcolumn into a subquery must not prevent the use of a projection
-- defined over that subcolumn (aggregate projection selection and normal projection key pruning).

SET enable_analyzer = 1;
SET optimize_use_projections = 1;

DROP TABLE IF EXISTS p_agg;
DROP TABLE IF EXISTS p_normal;

CREATE TABLE p_agg
(
    k UInt64,
    t Tuple(a UInt64, b UInt64),
    PROJECTION pa
    (
        SELECT getSubcolumn(t, 'a'), count()
        GROUP BY getSubcolumn(t, 'a')
    )
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

INSERT INTO p_agg SELECT number, (number % 100, number) FROM numbers(100000);

SELECT t.a, count() FROM (SELECT t FROM p_agg) GROUP BY t.a ORDER BY t.a LIMIT 3
SETTINGS optimize_push_subcolumns_into_subqueries = 0, force_optimize_projection = 1;

SELECT t.a, count() FROM (SELECT t FROM p_agg) GROUP BY t.a ORDER BY t.a LIMIT 3
SETTINGS optimize_push_subcolumns_into_subqueries = 1, force_optimize_projection = 1;

CREATE TABLE p_normal
(
    k UInt64,
    t Tuple(a UInt64, b UInt64),
    PROJECTION pa
    (
        SELECT * ORDER BY getSubcolumn(t, 'a')
    )
)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

INSERT INTO p_normal SELECT number, (number % 100, number) FROM numbers(100000);

SELECT count() FROM (SELECT t FROM p_normal) WHERE t.a = 33
SETTINGS optimize_push_subcolumns_into_subqueries = 0, force_optimize_projection = 1;

SELECT count() FROM (SELECT t FROM p_normal) WHERE t.a = 33
SETTINGS optimize_push_subcolumns_into_subqueries = 1, force_optimize_projection = 1;

DROP TABLE p_agg;
DROP TABLE p_normal;
