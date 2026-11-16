-- Equality across types is not transitive: `Int32` 0 equals `Enum8('same' = 0)`, which equals the `String` 'same',
-- which equals `Enum8('same' = 1)`, which equals `Int32` 1. The join order optimizer must not remove or add a
-- predicate through such a chain.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0;

CREATE TABLE e_t (t Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e_zero (e0 Enum8('same' = 0)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e_text (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e_one (e1 Enum8('same' = 1)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE e_v (v Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO e_t VALUES (0);
INSERT INTO e_zero VALUES ('same');
INSERT INTO e_text VALUES ('same');
INSERT INTO e_one VALUES ('same');
INSERT INTO e_v VALUES (1);

-- `t = v` is false, so no row matches. It must not be removed as made redundant by the chain.
SELECT 'kept', count()
FROM e_t JOIN e_zero ON t = e0 JOIN e_text ON e0 = s JOIN e_one ON s = e1 JOIN e_v ON e1 = v AND t = v
SETTINGS query_plan_optimize_join_order_limit = 10;
SELECT 'kept, written order', count()
FROM e_t JOIN e_zero ON t = e0 JOIN e_text ON e0 = s JOIN e_one ON s = e1 JOIN e_v ON e1 = v AND t = v
SETTINGS query_plan_optimize_join_order_limit = 0;

-- With `e_t` and `e_v` small, the optimizer joins them first, although only the chain connects them. It must not add
-- `t = v` to that join: every written predicate holds, so the row matches.
SELECT 'not added', count()
FROM e_t JOIN e_zero ON t = e0 JOIN e_text ON e0 = s JOIN e_one ON s = e1 JOIN e_v ON e1 = v
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10,
    param__internal_join_table_stat_hints = '{
        "e_t": { "cardinality": 2, "distinct_keys": { "t": 2 } },
        "e_v": { "cardinality": 2, "distinct_keys": { "v": 2 } },
        "e_zero": { "cardinality": 1000000000, "distinct_keys": { "e0": 1 } },
        "e_text": { "cardinality": 1000000000, "distinct_keys": { "s": 1 } },
        "e_one": { "cardinality": 1000000000, "distinct_keys": { "e1": 1 } }
    }';
SELECT 'not added, written order', count()
FROM e_t JOIN e_zero ON t = e0 JOIN e_text ON e0 = s JOIN e_one ON s = e1 JOIN e_v ON e1 = v
SETTINGS query_plan_optimize_join_order_limit = 0;

-- Classes of one type still give transitive predicates: `t = v` is added for the join of `i_t` with `i_v`, also when
-- a member is `Nullable`.
CREATE TABLE i_t (t Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE i_x (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE i_v (v Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO i_t VALUES (1), (2);
INSERT INTO i_x VALUES (1), (NULL);
INSERT INTO i_v VALUES (1), (3);

SELECT 'one type', count()
FROM i_t JOIN i_x ON t = x JOIN i_v ON x = v
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
    param__internal_join_table_stat_hints = '{
        "i_t": { "cardinality": 2, "distinct_keys": { "t": 2 } },
        "i_x": { "cardinality": 1000000000, "distinct_keys": { "x": 1 } },
        "i_v": { "cardinality": 2, "distinct_keys": { "v": 2 } }
    }';

SELECT trimLeft(replaceRegexpAll(explain, '__table\\d+\\.|[│└├─]', ''))
FROM (
    EXPLAIN SELECT count()
    FROM i_t JOIN i_x ON t = x JOIN i_v ON x = v
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10,
        query_plan_join_swap_table = 'false', use_hash_table_stats_for_join_reordering = 0,
        param__internal_join_table_stat_hints = '{
            "i_t": { "cardinality": 2, "distinct_keys": { "t": 2 } },
            "i_x": { "cardinality": 1000000000, "distinct_keys": { "x": 1 } },
            "i_v": { "cardinality": 2, "distinct_keys": { "v": 2 } }
        }'
)
WHERE explain LIKE '%Join conditions%';

-- A merge join gets a key only for a class of one type: `t = v` through the chain above is not one, and `kt = kv` is.
-- Every written predicate holds, so the row matches.
CREATE TABLE m_t (kt Int32, t Int32) ENGINE = MergeTree ORDER BY (kt, t) SETTINGS auto_statistics_types = '';
CREATE TABLE m_zero (k0 Int32, e0 Enum8('same' = 0)) ENGINE = MergeTree ORDER BY (k0, e0) SETTINGS auto_statistics_types = '';
CREATE TABLE m_text (ks Int32, s String) ENGINE = MergeTree ORDER BY (ks, s) SETTINGS auto_statistics_types = '';
CREATE TABLE m_one (k1 Int32, e1 Enum8('same' = 1)) ENGINE = MergeTree ORDER BY (k1, e1) SETTINGS auto_statistics_types = '';
CREATE TABLE m_v (kv Int32, v Int32) ENGINE = MergeTree ORDER BY (kv, v) SETTINGS auto_statistics_types = '';
INSERT INTO m_t VALUES (1, 0);
INSERT INTO m_zero VALUES (1, 'same');
INSERT INTO m_text VALUES (1, 'same');
INSERT INTO m_one VALUES (1, 'same');
INSERT INTO m_v VALUES (1, 1);

SELECT 'merge keys', count()
FROM m_t JOIN m_zero ON kt = k0 AND t = e0 JOIN m_text ON kt = ks AND e0 = s JOIN m_one ON kt = k1 AND s = e1
JOIN m_v ON kt = kv AND e1 = v
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10,
    query_plan_join_order_use_interesting_orders = 1, query_plan_join_swap_table = 'false', max_rows_in_set_to_optimize_join = 0,
    query_plan_join_shard_by_pk_ranges = 1, use_hash_table_stats_for_join_reordering = 0,
    param__internal_join_table_stat_hints = '{
        "m_t": { "cardinality": 1000000000, "distinct_keys": { "kt": 1000000000, "t": 1000000000 } },
        "m_v": { "cardinality": 1000000000, "distinct_keys": { "kv": 1000000000, "v": 1000000000 } },
        "m_zero": { "cardinality": 1000000000000, "distinct_keys": { "k0": 1, "e0": 1 } },
        "m_text": { "cardinality": 1000000000000, "distinct_keys": { "ks": 1, "s": 1 } },
        "m_one": { "cardinality": 1000000000000, "distinct_keys": { "k1": 1, "e1": 1 } }
    }';
SELECT 'merge keys, written order', count()
FROM m_t JOIN m_zero ON kt = k0 AND t = e0 JOIN m_text ON kt = ks AND e0 = s JOIN m_one ON kt = k1 AND s = e1
JOIN m_v ON kt = kv AND e1 = v
SETTINGS query_plan_optimize_join_order_limit = 0;

-- `f.k` is `UInt64`, `a.k` and `b.k` are `UInt32`: no class connects `a` with `b`, so no join order joins them first
-- without a condition, with any algorithm.
CREATE TABLE w_f (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE w_a (k UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE w_b (k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO w_f SELECT number FROM numbers(50000);
INSERT INTO w_a SELECT number FROM numbers(5000);
INSERT INTO w_b SELECT number FROM numbers(5000);

SELECT algorithm, countIf(explain LIKE '%ConstantJoin%') AS joins_without_condition
FROM
(
    SELECT 'dpsub' AS algorithm, explain FROM (EXPLAIN SELECT count() FROM w_f JOIN w_a ON w_f.k = w_a.k JOIN w_b ON w_f.k = w_b.k
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10)
    UNION ALL
    SELECT 'dphyp', explain FROM (EXPLAIN SELECT count() FROM w_f JOIN w_a ON w_f.k = w_a.k JOIN w_b ON w_f.k = w_b.k
        SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', query_plan_optimize_join_order_limit = 10)
    UNION ALL
    SELECT 'dpsize', explain FROM (EXPLAIN SELECT count() FROM w_f JOIN w_a ON w_f.k = w_a.k JOIN w_b ON w_f.k = w_b.k
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', query_plan_optimize_join_order_limit = 10)
    UNION ALL
    SELECT 'greedy', explain FROM (EXPLAIN SELECT count() FROM w_f JOIN w_a ON w_f.k = w_a.k JOIN w_b ON w_f.k = w_b.k
        SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10)
)
GROUP BY algorithm
ORDER BY algorithm;

SELECT 'mixed integer keys', count() FROM w_f JOIN w_a ON w_f.k = w_a.k JOIN w_b ON w_f.k = w_b.k;
