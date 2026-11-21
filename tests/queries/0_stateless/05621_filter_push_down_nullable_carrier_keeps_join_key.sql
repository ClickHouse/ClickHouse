-- When a Nullable conjunct is pushed down and the WHERE condition is also selected, one of the
-- remaining conjuncts carries the Nullable type. The equality between the two JOIN sides must
-- not be that conjunct, otherwise it is not merged into the JOIN as a key.

DROP TABLE IF EXISTS t_kept_cond_l;
DROP TABLE IF EXISTS t_kept_cond_r;

CREATE TABLE t_kept_cond_l (n Nullable(UInt64), a UInt64, s String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_kept_cond_r (b UInt64, t String) ENGINE = MergeTree ORDER BY b;

INSERT INTO t_kept_cond_l SELECT if(number % 7 = 5, NULL, number), number, toString(number) FROM numbers(100);
INSERT INTO t_kept_cond_r SELECT number, toString(number % 10) FROM numbers(100);

SET enable_parallel_replicas = 0;
SET query_plan_filter_push_down = 1, query_plan_merge_filter_into_join_condition = 1;

SELECT 'kept', countIf(explain ILIKE '%Type: cross%'), countIf(explain ILIKE '%Join conditions:%')
FROM (
    EXPLAIN actions = 1
    SELECT c, count()
    FROM (SELECT l.n AS n, l.a AS a, r.b AS b, l.s AS s, r.t AS t FROM t_kept_cond_l AS l CROSS JOIN t_kept_cond_r AS r)
    WHERE (n > 0 AND a = b AND endsWith(s, t)) AS c
    GROUP BY c);

SELECT 'removed', countIf(explain ILIKE '%Type: cross%'), countIf(explain ILIKE '%Join conditions:%')
FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT l.n AS n, l.a AS a, r.b AS b, l.s AS s, r.t AS t FROM t_kept_cond_l AS l CROSS JOIN t_kept_cond_r AS r)
    WHERE n > 0 AND a = b AND endsWith(s, t));

SELECT 'result', c, count()
FROM (SELECT l.n AS n, l.a AS a, r.b AS b, l.s AS s, r.t AS t FROM t_kept_cond_l AS l CROSS JOIN t_kept_cond_r AS r)
WHERE (n > 0 AND a = b AND endsWith(s, t)) AS c
GROUP BY c ORDER BY c;

DROP TABLE t_kept_cond_l;
DROP TABLE t_kept_cond_r;
