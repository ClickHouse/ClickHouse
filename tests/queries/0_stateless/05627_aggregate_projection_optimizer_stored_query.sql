-- the optimizer must read an aggregate projection by the column names its part stores

DROP TABLE IF EXISTS t_projection_stored_query;

CREATE TABLE t_projection_stored_query
(
    a UInt32,
    b String,
    c Nullable(UInt8),
    n Nested(k UInt8, v String),
    al ALIAS a % 2,
    PROJECTION p_alias (SELECT al, sum(al), SUM(a), count() IGNORE NULLS, any(al) RESPECT NULLS, sum(NULL), quantile(0.5)(a), countDistinct(b) GROUP BY al),
    PROJECTION p_tuple (SELECT sum(a) GROUP BY (b, c)),
    PROJECTION p_keys (SELECT sum(a) GROUP BY b, c),
    PROJECTION p_expressions (SELECT sum(a) GROUP BY a IN (1, 2), a + 1, toIntervalSecond(300) + toDateTime(a), arrayMap(x -> x + 1, n.k), arrayMap(x -> x + a, n.k), CAST(a, 'String'))
)
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_stored_query (a, b, c, n.k, n.v) VALUES
    (1, 'x', 1, [1], ['v']), (2, 'x', NULL, [2, 3], ['v', 'w']), (3, 'y', 1, [], []), (4, 'y', 1, [1], ['v']), (5, 'y', NULL, [1], ['v']), (6, 'z', 2, [], []);

SET optimize_use_projections = 1, force_optimize_projection = 1;

SELECT a % 2 AS k, sum(a % 2), sum(a), count(), any(a % 2) RESPECT NULLS, quantile(0.5)(a), uniqExact(b) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_alias';
SELECT al AS k, sum(al), sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_alias';
SELECT (b, c) AS k, sum(a) FROM t_projection_stored_query GROUP BY tuple(k) ORDER BY k SETTINGS force_optimize_projection_name = 'p_tuple', optimize_injective_functions_in_group_by = 0;
SELECT b, c, sum(a) FROM t_projection_stored_query GROUP BY b, c ORDER BY b, c SETTINGS force_optimize_projection_name = 'p_keys';
SELECT a IN (1, 2) AS k, sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_expressions';
SELECT a + 1 AS k, sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_expressions';
SELECT toUnixTimestamp(toIntervalSecond(300) + toDateTime(a) AS k), sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_expressions';
SELECT arrayMap(x -> x + 1, n.k) AS k, sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_expressions';
SELECT arrayMap(x -> x + a, n.k) AS k, sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_expressions';
SELECT CAST(a, 'String') AS k, sum(a) FROM t_projection_stored_query GROUP BY k ORDER BY k SETTINGS force_optimize_projection_name = 'p_expressions';

SELECT name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_stored_query' AND active
ORDER BY name, column_position;

DROP TABLE t_projection_stored_query;

-- the WHERE and the key of the projection share an expression
DROP TABLE IF EXISTS t_projection_stored_query_where;
CREATE TABLE t_projection_stored_query_where (a UInt32, b UInt32, PROJECTION p (SELECT sum(b) WHERE a + 1 > 2 GROUP BY a + 1))
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_stored_query_where VALUES (1, 10), (2, 20), (3, 30);
SELECT a + 1 AS k, sum(b) FROM t_projection_stored_query_where WHERE a + 1 > 2 GROUP BY k ORDER BY k;
DROP TABLE t_projection_stored_query_where;

-- a setting of the query must not rewrite the stored query: the projection keeps its IN
DROP TABLE IF EXISTS t_projection_stored_query_in;
CREATE TABLE t_projection_stored_query_in (a UInt32, b UInt32, PROJECTION p (SELECT sum(a) GROUP BY b, a IN (SELECT 1)))
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_stored_query_in VALUES (1, 10), (2, 10), (3, 30);
SELECT b, sum(a) FROM t_projection_stored_query_in GROUP BY b ORDER BY b SETTINGS allow_correlated_subqueries = 1, rewrite_in_to_join = 1;
DROP TABLE t_projection_stored_query_in;

-- the settings below change the function of the query, so the stored states do not fit it
SET force_optimize_projection = 0;

DROP TABLE IF EXISTS t_projection_stored_query_distinct;
CREATE TABLE t_projection_stored_query_distinct (k UInt8, s String, c UInt8, PROJECTION p (SELECT countDistinctIf(s, c) GROUP BY k))
ENGINE = MergeTree ORDER BY k
SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_stored_query_distinct VALUES (1, 'a', 1), (1, 'a', 1), (1, 'b', 1), (1, 'c', 0), (2, 'e', 1);
SELECT k, countDistinctIf(s, c) FROM t_projection_stored_query_distinct GROUP BY k ORDER BY k SETTINGS rewrite_count_distinct_if_with_count_distinct_implementation = 1;
DROP TABLE t_projection_stored_query_distinct;

DROP TABLE IF EXISTS t_projection_stored_query_null_in;
CREATE TABLE t_projection_stored_query_null_in (a Nullable(Int32), x UInt32, PROJECTION p (SELECT sum(x) GROUP BY nullIn(a, (1, NULL))))
ENGINE = MergeTree ORDER BY x
SETTINGS materialize_projections_on_insert = 1;
INSERT INTO t_projection_stored_query_null_in VALUES (1, 1), (NULL, 10), (2, 100);
SELECT nullIn(a, (1, NULL)) AS k, sum(x) FROM t_projection_stored_query_null_in GROUP BY k ORDER BY k SETTINGS transform_null_in = 1;
DROP TABLE t_projection_stored_query_null_in;
