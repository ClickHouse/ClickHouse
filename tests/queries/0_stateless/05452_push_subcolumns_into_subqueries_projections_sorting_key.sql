-- A function-form carrier (`length(s)`, `count(n)`) over a column used by a projection or by the
-- sorting key beyond the primary key must not be lowered to a subcolumn read in the subquery:
-- projection selection and reading in order match these expressions literally.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_proj_group;
CREATE TABLE t_push_proj_group
(
    s String,
    PROJECTION p (SELECT length(s) AS l, count() AS c GROUP BY length(s))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_proj_group SELECT repeat('a', number % 10) FROM numbers(1000);

SELECT length(s), count()
FROM (SELECT s FROM t_push_proj_group)
GROUP BY length(s)
ORDER BY length(s)
SETTINGS force_optimize_projection = 1;

DROP TABLE t_push_proj_group;

DROP TABLE IF EXISTS t_push_proj_state;
CREATE TABLE t_push_proj_state
(
    k UInt8,
    n Nullable(UInt64),
    PROJECTION p (SELECT k, count(n) GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_proj_state SELECT number % 3, if(number % 2 = 0, NULL, number) FROM numbers(1000);

SELECT k, count(n)
FROM (SELECT k, n FROM t_push_proj_state)
GROUP BY k
ORDER BY k
SETTINGS force_optimize_projection = 1;

DROP TABLE t_push_proj_state;

DROP TABLE IF EXISTS t_push_sorting_key;
CREATE TABLE t_push_sorting_key (id UInt64, s String)
ENGINE = MergeTree ORDER BY (id, length(s)) PRIMARY KEY id;
INSERT INTO t_push_sorting_key SELECT 0, repeat('a', number % 8) FROM numbers(1000);

SELECT length(s)
FROM (SELECT s, id FROM t_push_sorting_key)
WHERE id = 0
ORDER BY length(s) LIMIT 1;

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT length(s)
    FROM (SELECT s, id FROM t_push_sorting_key)
    WHERE id = 0
    ORDER BY length(s) LIMIT 1
)
WHERE explain LIKE '%Read type: InOrder%';

DROP TABLE t_push_sorting_key;
