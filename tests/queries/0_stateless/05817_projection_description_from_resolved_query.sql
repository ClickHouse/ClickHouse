-- the columns of a projection part have the names of the query as the analyzer resolves it

DROP TABLE IF EXISTS t_projection_resolved;

CREATE TABLE t_projection_resolved
(
    a Int32,
    b Nullable(Int32),
    c String,
    m Int64 MATERIALIZED a * 2,
    al ALIAS a + 1,
    -- the names that do not change
    PROJECTION p_same (SELECT a % 10 AS k, count(*), count(1), sum(1), countDistinct(c), sumIf(a, a > 1 + 1), sum(al) GROUP BY k),
    -- the function that the analyzer resolves gives the name
    PROJECTION p_resolved (SELECT a, any(b) RESPECT NULLS, count() IGNORE NULLS, sum(NULL), median(a) GROUP BY a),
    -- a constant key is not stored, and the last key takes its place
    PROJECTION p_keys (SELECT c, max(b) GROUP BY 'x', c, a),
    -- an asterisk has no MATERIALIZED and no ALIAS column
    PROJECTION p_asterisk (SELECT * ORDER BY c),
    -- with WHERE the columns of the table go first
    PROJECTION p_where (SELECT a + 1, upper(c), al WHERE a > 0 ORDER BY c)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_resolved VALUES (1, NULL, 'x'), (2, 5, 'y'), (12, 7, 'y');

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_resolved' AND active
ORDER BY name, column_position;

SELECT a % 10 AS k, count(), uniqExact(c), sum(al) FROM t_projection_resolved GROUP BY k ORDER BY k
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_same';

SELECT a, any(b) RESPECT NULLS, quantile(a) FROM t_projection_resolved GROUP BY a ORDER BY a
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_resolved';

SELECT c, max(b) FROM t_projection_resolved GROUP BY c ORDER BY c
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_keys';

DROP TABLE t_projection_resolved;

-- a scalar subquery has the value of the time when the projection is created
DROP TABLE IF EXISTS t_projection_resolved_limit;
DROP TABLE IF EXISTS t_projection_resolved_scalar;

CREATE TABLE t_projection_resolved_limit (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_projection_resolved_limit VALUES (1);

CREATE TABLE t_projection_resolved_scalar
(
    a Int32,
    PROJECTION p (SELECT a > (SELECT max(x) FROM t_projection_resolved_limit) AS k, count() GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_resolved_limit VALUES (100);
INSERT INTO t_projection_resolved_scalar VALUES (0), (2), (200);

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_resolved_scalar' AND active
ORDER BY column_position;

SELECT k, c FROM (SELECT `greater(a, _CAST(1, 'Nullable(Int32)'))` AS k, finalizeAggregation(`count()`) AS c FROM mergeTreeProjection(currentDatabase(), t_projection_resolved_scalar, p)) ORDER BY k;

-- a scalar subquery that is not a simple value is also stored as its value
DROP TABLE IF EXISTS t_projection_resolved_array;
CREATE TABLE t_projection_resolved_array (a Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS materialize_projections_on_insert = 1;
ALTER TABLE t_projection_resolved_array ADD PROJECTION p (SELECT has((SELECT groupArray(x) FROM (SELECT x FROM t_projection_resolved_limit ORDER BY x)), a) AS k, count() GROUP BY k);
INSERT INTO t_projection_resolved_array VALUES (1), (100), (300);

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_resolved_array' AND active
ORDER BY column_position;

SELECT k, c FROM (SELECT `has(_CAST([1, 100], 'Array(Int32)'), a)` AS k, finalizeAggregation(`count()`) AS c FROM mergeTreeProjection(currentDatabase(), t_projection_resolved_array, p)) ORDER BY k;

-- a scalar subquery inside a subquery of IN also has its value
DROP TABLE IF EXISTS t_projection_resolved_nested;
CREATE TABLE t_projection_resolved_nested
(
    a Int32,
    PROJECTION p (SELECT a IN (SELECT x FROM t_projection_resolved_limit WHERE x > (SELECT avg(x) FROM t_projection_resolved_limit)) AS k, count() GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_resolved_nested VALUES (1), (100), (300);

SELECT * EXCEPT (`count()`), finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), t_projection_resolved_nested, p) ORDER BY ALL;

DROP TABLE t_projection_resolved_nested;
DROP TABLE t_projection_resolved_array;
DROP TABLE t_projection_resolved_scalar;
DROP TABLE t_projection_resolved_limit;

-- the columns of a subquery keep the names of their tables
DROP TABLE IF EXISTS t_projection_resolved_left;
DROP TABLE IF EXISTS t_projection_resolved_right;
DROP TABLE IF EXISTS t_projection_resolved_join;

CREATE TABLE t_projection_resolved_left (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_projection_resolved_right (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_resolved_left VALUES (1, 10);
INSERT INTO t_projection_resolved_right VALUES (1, 20);

CREATE TABLE t_projection_resolved_join
(
    a Int32,
    PROJECTION p
    (
        SELECT a
        WHERE a IN (SELECT r.v FROM t_projection_resolved_left AS l JOIN t_projection_resolved_right AS r ON l.k = r.k)
        ORDER BY a
    )
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_resolved_join VALUES (10), (20), (30);

SELECT a FROM mergeTreeProjection(currentDatabase(), t_projection_resolved_join, p) ORDER BY a;

DROP TABLE t_projection_resolved_join;
DROP TABLE t_projection_resolved_right;
DROP TABLE t_projection_resolved_left;
