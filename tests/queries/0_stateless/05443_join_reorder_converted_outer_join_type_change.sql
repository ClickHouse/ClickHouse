-- https://github.com/ClickHouse/ClickHouse/issues/127331: with join_use_nulls = 1, an outer join converted to INNER and
-- reordered so that its null-supplying side is joined first failed with "Cannot fold actions for projection".

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;

CREATE TABLE a (k String, t String, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE b (k String, t String, x String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE c (x String, t String, v String) ENGINE = MergeTree ORDER BY x;
INSERT INTO a VALUES ('k1', 't1', 'a1'), ('k2', 't1', 'a2'), ('k3', 't2', 'a3');
INSERT INTO b VALUES ('k1', 't1', 'x1'), ('k2', 't1', 'x2'), ('k3', 't2', 'x1');
INSERT INTO c VALUES ('x1', 't1', 'x'), ('x2', 't1', 'y'), ('x1', 't2', 'x');

SELECT a.v, b.x, c.x, c.v, toTypeName(c.x)
FROM a JOIN b ON a.k = b.k AND a.t = b.t LEFT JOIN c ON b.x = c.x AND b.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_randomize = 0;

SELECT a.v, b.x, c.x, c.v
FROM a JOIN b ON a.k = b.k AND a.t = b.t LEFT JOIN c ON b.x = c.x AND b.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'dpsize', query_plan_optimize_join_order_randomize = 0;

SELECT a.v, b.x, c.x, c.v
FROM a JOIN b ON a.k = b.k AND a.t = b.t LEFT JOIN c ON b.x = c.x AND b.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'dphyp', query_plan_optimize_join_order_randomize = 13;

SELECT a.v, b.x, c.x, c.v
FROM a JOIN b ON a.k = b.k AND a.t = b.t LEFT JOIN c ON b.x = c.x AND b.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_randomize = 13,
    query_plan_optimize_join_order_conflict_detector = '';

SELECT a.v, b.x, c.x, c.v
FROM a JOIN b ON a.k = b.k AND a.t = b.t LEFT JOIN c ON b.x = c.x AND b.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_randomize = 13,
    query_plan_optimize_join_order_conflict_detector = 'a';

SELECT a.v, b.x, c.x, c.v
FROM a JOIN b ON a.k = b.k AND a.t = b.t LEFT JOIN c ON b.x = c.x AND b.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_randomize = 13,
    query_plan_optimize_join_order_conflict_detector = 'c';

SELECT ab.v, ab.x, c.x, c.v
FROM c RIGHT JOIN (SELECT a.v AS v, a.t AS t, b.x AS x FROM a JOIN b ON a.k = b.k AND a.t = b.t) AS ab ON ab.x = c.x AND ab.t = c.t
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_randomize = 0;

DROP TABLE a;
DROP TABLE c;
CREATE TABLE a (k String, t String, v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE c (x String, t String, v String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE d (w String) ENGINE = MergeTree ORDER BY w;
INSERT INTO a VALUES ('x1', 't1', 'a1'), ('x2', 't1', 'a2'), ('x1', 't2', 'a3');
INSERT INTO c VALUES ('x1', 't1', 'x'), ('x2', 't1', 'y');
INSERT INTO d VALUES ('x1'), ('x2');

-- `c` is joined with `a` before `b` through the equivalence `a.k = d.w = b.x = c.x`.
SELECT a.v, b.x, c.x, c.v
FROM a JOIN d ON a.k = d.w JOIN b ON d.w = b.x AND a.t = b.t LEFT JOIN c ON b.x = c.x
WHERE c.v = 'x' ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1, enable_join_transitive_predicates = 1,
    query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0,
    query_plan_optimize_join_order_algorithm = 'dpsize', query_plan_optimize_join_order_randomize = 82;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP TABLE d;
