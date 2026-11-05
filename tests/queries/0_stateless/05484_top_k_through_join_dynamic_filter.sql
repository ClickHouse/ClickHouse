-- Tags: no-parallel-replicas
-- no-parallel-replicas: the rewrite is skipped with parallel replicas

DROP TABLE IF EXISTS t_tj_left;
DROP TABLE IF EXISTS t_tj_right;

CREATE TABLE t_tj_left (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_tj_right (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_tj_left SELECT number, sipHash64(number) FROM numbers(1000);
INSERT INTO t_tj_right SELECT number, number FROM numbers(100);

SET optimize_read_in_order = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_top_k_through_join = 1;

SELECT '-- the sort below the join gets the top-K filter';
SELECT count() FROM (EXPLAIN actions = 1 SELECT l.k, r.v FROM t_tj_left AS l LEFT JOIN t_tj_right AS r ON l.id = r.id ORDER BY l.k LIMIT 10)
WHERE explain LIKE '%__topKFilter%';

SELECT '-- a chain of LEFT JOIN: the sort reaches the read';
SELECT count() FROM (EXPLAIN actions = 1 SELECT l.k FROM t_tj_left AS l LEFT JOIN t_tj_right AS r1 ON r1.id = l.id LEFT JOIN t_tj_right AS r2 ON r2.id = l.id ORDER BY l.k LIMIT 10)
WHERE explain LIKE '%__topKFilter%';

SELECT '-- same rows as without the rewrite';
SELECT
    (SELECT groupArray((k, v)) FROM (SELECT l.k AS k, r.v AS v FROM t_tj_left AS l LEFT JOIN t_tj_right AS r ON l.id = r.id ORDER BY l.k LIMIT 10))
    = (SELECT groupArray((k, v)) FROM (SELECT l.k AS k, r.v AS v FROM t_tj_left AS l LEFT JOIN t_tj_right AS r ON l.id = r.id ORDER BY l.k LIMIT 10 SETTINGS query_plan_top_k_through_join = 0));
SELECT
    (SELECT groupArray((k, v)) FROM (SELECT l.k AS k, r.v AS v FROM t_tj_right AS r RIGHT JOIN t_tj_left AS l ON l.id = r.id ORDER BY l.k DESC LIMIT 5 OFFSET 3))
    = (SELECT groupArray((k, v)) FROM (SELECT l.k AS k, r.v AS v FROM t_tj_right AS r RIGHT JOIN t_tj_left AS l ON l.id = r.id ORDER BY l.k DESC LIMIT 5 OFFSET 3 SETTINGS query_plan_top_k_through_join = 0));
SELECT
    (SELECT groupArray((k, v)) FROM (SELECT l.k AS k, r2.v AS v FROM t_tj_left AS l LEFT JOIN t_tj_right AS r1 ON r1.id = l.id LEFT JOIN t_tj_right AS r2 ON r2.id = l.id + 1 ORDER BY l.k LIMIT 10))
    = (SELECT groupArray((k, v)) FROM (SELECT l.k AS k, r2.v AS v FROM t_tj_left AS l LEFT JOIN t_tj_right AS r1 ON r1.id = l.id LEFT JOIN t_tj_right AS r2 ON r2.id = l.id + 1 ORDER BY l.k LIMIT 10 SETTINGS query_plan_top_k_through_join = 0));

DROP TABLE t_tj_left;
DROP TABLE t_tj_right;
