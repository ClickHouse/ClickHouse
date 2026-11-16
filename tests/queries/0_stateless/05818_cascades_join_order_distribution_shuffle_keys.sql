-- With `query_plan_join_order_distribution`, DPsub chooses the key of a shuffle join among the columns of a class, so
-- that a later join is keyed on a column that a shuffle already hashed the rows by. Cascades advertises only the key
-- columns of a shuffle as its partitioning. The cases after the first one are queries where columns are equal on
-- some rows only: the result must not change with the setting.

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
-- The distribution-aware search runs only with the default `join_algorithm`.
SET join_algorithm = 'direct,hash,ie_join';
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_join_transitive_predicates = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET join_use_nulls = 0;
SET explain_query_plan_default = 'legacy';
-- The test profile of CI sets these limits, which keep aggregations and joins on one node.
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET param__internal_cascades_cluster_node_count = 8;
-- A large cost of a hash table makes the broadcast joins too expensive, so the joins shuffle.
SET param__internal_cascades_cost_config = '{"hash_table_build_factor":1000000}';

CREATE TABLE q_a (a Int32, a_value Int32) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
CREATE TABLE q_b (b Int32, b_value Int32) ENGINE = MergeTree ORDER BY b SETTINGS auto_statistics_types = '';
CREATE TABLE q_c (c Int32, c_value Int32) ENGINE = MergeTree ORDER BY c SETTINGS auto_statistics_types = '';
CREATE TABLE q_d (d Int32, d_value Int32) ENGINE = MergeTree ORDER BY d SETTINGS auto_statistics_types = '';
INSERT INTO q_a SELECT number % 10, number FROM numbers(20);
INSERT INTO q_b SELECT number % 12, number FROM numbers(20);
INSERT INTO q_c SELECT number % 14, number FROM numbers(20);
INSERT INTO q_d SELECT number % 16, number FROM numbers(20);

SET param__internal_join_table_stat_hints = '{
    "q_a": { "cardinality": 10000000, "avg_row_bytes": 8, "distinct_keys": { "a": 10000000 } },
    "q_b": { "cardinality": 20000000, "avg_row_bytes": 8, "distinct_keys": { "b": 10000000 } },
    "q_c": { "cardinality": 30000000, "avg_row_bytes": 8, "distinct_keys": { "c": 10000000 } },
    "q_d": { "cardinality": 40000000, "avg_row_bytes": 8, "distinct_keys": { "d": 10000000 } }
}';

-- Each table is shuffled once: every join is keyed on a column that the shuffle below it hashed the rows by.
SELECT '-- Shuffle keys';
EXPLAIN
SELECT sum(a_value + b_value + c_value + d_value)
FROM q_a JOIN q_b ON a = b JOIN q_c ON b = c JOIN q_d ON a = d
SETTINGS query_plan_join_order_distribution = 1;

SELECT sum(a_value + b_value + c_value + d_value)
FROM q_a JOIN q_b ON a = b JOIN q_c ON b = c JOIN q_d ON a = d
SETTINGS query_plan_join_order_distribution = 1;
SELECT sum(a_value + b_value + c_value + d_value)
FROM q_a JOIN q_b ON a = b JOIN q_c ON b = c JOIN q_d ON a = d
SETTINGS make_distributed_plan = 0;

CREATE TABLE f_a (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
CREATE TABLE f_b (b Int32) ENGINE = MergeTree ORDER BY b SETTINGS auto_statistics_types = '';
CREATE TABLE f_t (a Int32, b Int32) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
CREATE TABLE f_c (c Int32) ENGINE = MergeTree ORDER BY c SETTINGS auto_statistics_types = '';
CREATE TABLE f_d (d Int32) ENGINE = MergeTree ORDER BY d SETTINGS auto_statistics_types = '';
INSERT INTO f_a VALUES (1);
INSERT INTO f_b VALUES (1);
INSERT INTO f_t VALUES (0, 1);
INSERT INTO f_c VALUES (0), (1);
INSERT INTO f_d VALUES (0), (1);

SET param__internal_join_table_stat_hints = '{
    "f_a": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "a": 500000000 } },
    "f_b": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "b": 500000000 } },
    "f_t": { "cardinality": 500000000, "avg_row_bytes": 8, "distinct_keys": { "a": 500000000, "b": 500000000 } },
    "f_c": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "c": 500000000 } },
    "f_d": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "d": 500000000 } }
}';

-- `a = b` holds in the first input of the `UNION ALL` only. 2 rows.
SELECT 'union all', query_plan_join_order_distribution, count()
FROM (SELECT a, b FROM f_a INNER JOIN f_b ON a = b UNION ALL SELECT a, b FROM f_t) AS u
INNER JOIN f_c ON u.a = f_c.c
INNER JOIN f_d ON u.b = f_d.d
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 0;
SELECT 'union all', query_plan_join_order_distribution, count()
FROM (SELECT a, b FROM f_a INNER JOIN f_b ON a = b UNION ALL SELECT a, b FROM f_t) AS u
INNER JOIN f_c ON u.a = f_c.c
INNER JOIN f_d ON u.b = f_d.d
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 1;

-- Grouping sets fill the key that a set does not have with its default value. 2 rows.
SELECT 'grouping sets', query_plan_join_order_distribution, count()
FROM (SELECT a, b FROM f_a INNER JOIN f_b ON a = b GROUP BY GROUPING SETS ((a), (b))) AS g
INNER JOIN f_c ON g.a = f_c.c
INNER JOIN f_d ON g.b = f_d.d
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 0;
SELECT 'grouping sets', query_plan_join_order_distribution, count()
FROM (SELECT a, b FROM f_a INNER JOIN f_b ON a = b GROUP BY GROUPING SETS ((a), (b))) AS g
INNER JOIN f_c ON g.a = f_c.c
INNER JOIN f_d ON g.b = f_d.d
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 1;

-- An outer join fills an unmatched side with default values: 1 for `Enum8('one' = 1)`, 0 for `Int32`. 1 row.
CREATE TABLE r_l (l Int32) ENGINE = MergeTree ORDER BY l SETTINGS auto_statistics_types = '';
CREATE TABLE r_e (x Int32, e Enum8('one' = 1)) ENGINE = MergeTree ORDER BY x SETTINGS auto_statistics_types = '';
CREATE TABLE r_n (n Int32) ENGINE = MergeTree ORDER BY n SETTINGS auto_statistics_types = '';
CREATE TABLE r_t (t Int32) ENGINE = MergeTree ORDER BY t SETTINGS auto_statistics_types = '';
CREATE TABLE r_v (v Int32) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '';
CREATE TABLE r_w (w Int32) ENGINE = MergeTree ORDER BY w SETTINGS auto_statistics_types = '';
INSERT INTO r_l VALUES (2);
INSERT INTO r_e VALUES (1, 'one');
INSERT INTO r_n VALUES (1);
INSERT INTO r_t VALUES (1);
INSERT INTO r_v VALUES (1);
INSERT INTO r_w VALUES (0);

SET param__internal_join_table_stat_hints = '{
    "r_l": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "l": 500000000 } },
    "r_e": { "cardinality": 500000000, "avg_row_bytes": 5, "distinct_keys": { "x": 500000000, "e": 1 } },
    "r_n": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "n": 500000000 } },
    "r_t": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "t": 500000000 } },
    "r_v": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "v": 500000000 } },
    "r_w": { "cardinality": 500000000, "avg_row_bytes": 4, "distinct_keys": { "w": 500000000 } }
}';

SELECT 'outer join defaults', query_plan_join_order_distribution, count()
FROM (SELECT r.e, r.n FROM r_l LEFT ALL JOIN (SELECT r_e.x, r_e.e, r_n.n FROM r_e INNER ALL JOIN r_n ON r_e.e = r_n.n) AS r ON r_l.l = r.x) AS u
INNER ALL JOIN r_t ON u.e = r_t.t
INNER ALL JOIN r_v ON r_t.t = r_v.v
INNER ALL JOIN r_w ON u.n = r_w.w
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 0;
SELECT 'outer join defaults', query_plan_join_order_distribution, count()
FROM (SELECT r.e, r.n FROM r_l LEFT ALL JOIN (SELECT r_e.x, r_e.e, r_n.n FROM r_e INNER ALL JOIN r_n ON r_e.e = r_n.n) AS r ON r_l.l = r.x) AS u
INNER ALL JOIN r_t ON u.e = r_t.t
INNER ALL JOIN r_v ON r_t.t = r_v.v
INNER ALL JOIN r_w ON u.n = r_w.w
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 1;

-- Equality across types is not transitive: `t` is 0 and `v` is 1, joined through `Enum8('same' = 0)`, `String` and
-- `Enum8('same' = 1)`. 1 row.
CREATE TABLE e_zero (e0 Enum8('same' = 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE e_text (s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE e_one (e1 Enum8('same' = 1)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE e_t (t Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE e_v (v Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE e_k (k Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE e_w (w Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO e_zero VALUES ('same');
INSERT INTO e_text VALUES ('same');
INSERT INTO e_one VALUES ('same');
INSERT INTO e_t VALUES (0);
INSERT INTO e_v VALUES (1);
INSERT INTO e_k VALUES (0);
INSERT INTO e_w VALUES (1);

SET param__internal_join_table_stat_hints = '{
    "e_zero": { "cardinality": 500000000, "distinct_keys": { "e0": 500000000 } },
    "e_text": { "cardinality": 500000000, "distinct_keys": { "s": 500000000 } },
    "e_one": { "cardinality": 500000000, "distinct_keys": { "e1": 500000000 } },
    "e_t": { "cardinality": 500000000, "distinct_keys": { "t": 500000000 } },
    "e_v": { "cardinality": 500000000, "distinct_keys": { "v": 500000000 } },
    "e_k": { "cardinality": 500000000, "distinct_keys": { "k": 500000000 } },
    "e_w": { "cardinality": 500000000, "distinct_keys": { "w": 500000000 } }
}';

SELECT 'types of a class', query_plan_join_order_distribution, count()
FROM e_zero
INNER ALL JOIN e_text ON e0 = s
INNER ALL JOIN e_one ON s = e1
INNER ALL JOIN e_t ON e0 = t
INNER ALL JOIN e_v ON e1 = v
INNER ALL JOIN e_k ON t = k
INNER ALL JOIN e_w ON v = w
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 0;
SELECT 'types of a class', query_plan_join_order_distribution, count()
FROM e_zero
INNER ALL JOIN e_text ON e0 = s
INNER ALL JOIN e_one ON s = e1
INNER ALL JOIN e_t ON e0 = t
INNER ALL JOIN e_v ON e1 = v
INNER ALL JOIN e_k ON t = k
INNER ALL JOIN e_w ON v = w
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 1;

-- `USING` casts `a` and `b`, equal `Date` values, to `DateTime` in two time zones under the same names. 1 row.
CREATE TABLE dt_a (a Date) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE dt_b (b Date) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE dt_utc (a DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE dt_ams (b DateTime('Europe/Amsterdam')) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE dt_c (c DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE dt_d (d DateTime('Europe/Amsterdam')) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO dt_a VALUES ('2024-01-01');
INSERT INTO dt_b VALUES ('2024-01-01');
INSERT INTO dt_utc VALUES ('2024-01-01 00:00:00');
INSERT INTO dt_ams VALUES ('2024-01-01 00:00:00');
INSERT INTO dt_c VALUES ('2024-01-01 00:00:00');
INSERT INTO dt_d VALUES ('2024-01-01 00:00:00');

SET param__internal_join_table_stat_hints = '{
    "dt_a": { "cardinality": 500000000, "distinct_keys": { "a": 500000000 } },
    "dt_b": { "cardinality": 500000000, "distinct_keys": { "b": 500000000 } },
    "dt_utc": { "cardinality": 500000000, "distinct_keys": { "a": 500000000 } },
    "dt_ams": { "cardinality": 500000000, "distinct_keys": { "b": 500000000 } },
    "dt_c": { "cardinality": 500000000, "distinct_keys": { "c": 500000000 } },
    "dt_d": { "cardinality": 500000000, "distinct_keys": { "d": 500000000 } }
}';

SELECT 'using casts', query_plan_join_order_distribution, count()
FROM
(
    SELECT a, b FROM
    (
        SELECT a, b FROM (SELECT dt_a.a, dt_b.b FROM dt_a INNER ALL JOIN dt_b ON dt_a.a = dt_b.b) AS q
        INNER ALL JOIN dt_utc USING (a)
    ) AS q1
    INNER ALL JOIN dt_ams USING (b)
) AS u
INNER ALL JOIN dt_c ON u.a = dt_c.c
INNER ALL JOIN dt_d ON u.b = dt_d.d
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 0;
SELECT 'using casts', query_plan_join_order_distribution, count()
FROM
(
    SELECT a, b FROM
    (
        SELECT a, b FROM (SELECT dt_a.a, dt_b.b FROM dt_a INNER ALL JOIN dt_b ON dt_a.a = dt_b.b) AS q
        INNER ALL JOIN dt_utc USING (a)
    ) AS q1
    INNER ALL JOIN dt_ams USING (b)
) AS u
INNER ALL JOIN dt_c ON u.a = dt_c.c
INNER ALL JOIN dt_d ON u.b = dt_d.d
GROUP BY getSetting('query_plan_join_order_distribution') AS query_plan_join_order_distribution
SETTINGS query_plan_join_order_distribution = 1;
