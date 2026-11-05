-- The optimizer reads the columns of an aggregate projection by the names its metadata stores them under.
-- Those names are the expressions as they are written in the projection query, after the table ALIAS
-- columns are substituted: `plus(a, 1)` for `al`, one key `tuple(b, c)` for `GROUP BY (b, c)`,
-- `count() IGNORE NULLS` and `any(a) RESPECT NULLS` with their nulls action, `uniqExact(b)` for `countDistinct(b)`.

DROP TABLE IF EXISTS t_projection_stored_names;

CREATE TABLE t_projection_stored_names
(
    a UInt32,
    b String,
    c Nullable(UInt8),
    n Nested(k UInt8, v String),
    al ALIAS a + 1,
    PROJECTION p_alias (SELECT al, sum(al), SUM(a), count() IGNORE NULLS, any(a) RESPECT NULLS, sum(NULL), quantile(0.5)(a), countDistinct(b) GROUP BY al),
    PROJECTION p_tuple (SELECT sum(a) GROUP BY (b, c)),
    PROJECTION p_keys (SELECT sum(a) GROUP BY b, c),
    PROJECTION p_expressions (SELECT sum(a) GROUP BY a IN (1, 2), a + 1, toIntervalSecond(300) + toDateTime(a), arrayMap(x -> x + 1, n.k), CAST(a, 'String'))
)
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_stored_names (a, b, c, n.k, n.v) VALUES (1, 'x', 1, [1], ['v']), (2, 'x', NULL, [2, 3], ['v', 'w']), (3, 'y', 1, [], []);

SET optimize_use_projections = 1, force_optimize_projection = 1;

SELECT a + 1 AS k, sum(a + 1), sum(a), count(), any(a) RESPECT NULLS, quantile(0.5)(a), uniqExact(b) FROM t_projection_stored_names GROUP BY k ORDER BY k;
SELECT al AS k, sum(al) FROM t_projection_stored_names GROUP BY k ORDER BY k;
SELECT (b, c) AS k, sum(a) FROM t_projection_stored_names GROUP BY k ORDER BY k;
SELECT b, c, sum(a) FROM t_projection_stored_names GROUP BY b, c ORDER BY b, c;
SELECT a IN (1, 2) AS k, sum(a) FROM t_projection_stored_names GROUP BY k ORDER BY k;
SELECT a + 1 AS k, sum(a) FROM t_projection_stored_names GROUP BY k ORDER BY k;
SELECT CAST(a, 'String') AS k, sum(a) FROM t_projection_stored_names GROUP BY k ORDER BY k;

SELECT name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_stored_names' AND active
ORDER BY name, column_position;

DROP TABLE t_projection_stored_names;
