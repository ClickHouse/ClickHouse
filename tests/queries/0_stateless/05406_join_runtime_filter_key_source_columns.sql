-- A join key computed below the join for the runtime filter must not leave its source column as a join input.

-- Pinned because the test asserts on the join inputs.
SET explain_query_plan_default = 'pretty';
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_remove_unused_columns = 1;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;

CREATE TABLE l (k UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE r (k UInt64, x UInt64) ENGINE = Memory;
INSERT INTO l SELECT number % 100, number FROM numbers(10000);
INSERT INTO r SELECT number, number FROM numbers(50);

SELECT '-- build side computed key';
SELECT extract(explain, 'Input \\((?:left|right)\\): .*')
FROM (EXPLAIN SELECT sum(v) FROM l JOIN r ON l.k = r.k + 1)
WHERE explain LIKE '%Input (%';
SELECT count() > 0 FROM (EXPLAIN SELECT sum(v) FROM l JOIN r ON l.k = r.k + 1) WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT sum(v) FROM l JOIN r ON l.k = r.k + 1;
SELECT sum(v) FROM l JOIN r ON l.k = r.k + 1 SETTINGS enable_join_runtime_filters = 0;

SELECT '-- probe side computed key';
SELECT extract(explain, 'Input \\((?:left|right)\\): .*')
FROM (EXPLAIN SELECT sum(v) FROM l JOIN r ON l.k + 1 = r.k)
WHERE explain LIKE '%Input (%';
SELECT count() > 0 FROM (EXPLAIN SELECT sum(v) FROM l JOIN r ON l.k + 1 = r.k) WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT sum(v) FROM l JOIN r ON l.k + 1 = r.k;
SELECT sum(v) FROM l JOIN r ON l.k + 1 = r.k SETTINGS enable_join_runtime_filters = 0;

CREATE TABLE orders (o_custkey UInt64, o_price UInt64) ENGINE = MergeTree ORDER BY o_custkey;
CREATE TABLE customer (c_custkey UInt64, c_nationkey UInt64) ENGINE = MergeTree ORDER BY c_custkey;
CREATE TABLE nation (n_nationkey UInt64, n_name String) ENGINE = MergeTree ORDER BY n_nationkey;
INSERT INTO orders SELECT number % 1000, number FROM numbers(100000);
INSERT INTO customer SELECT number, number % 3 FROM numbers(300);
INSERT INTO nation VALUES (0, 'a'), (1, 'b'), (2, 'c');

SELECT '-- merged subquery projection, inputs listing a column twice';
SELECT countIf(length(cols) != length(arrayDistinct(cols)))
FROM
(
    SELECT splitByString(', ', extract(explain, 'Input \\((?:left|right)\\): (.*)')) AS cols
    FROM
    (
        EXPLAIN SELECT sum(o_price)
        FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'a') AS cn ON c_custkey = o_custkey
        SETTINGS query_plan_merge_expression_into_join = 1
    )
    WHERE explain LIKE '%Input (%'
);
-- Both joins build a runtime filter, including the outer one on the merged alias key.
SELECT count() >= 2
FROM
(
    EXPLAIN SELECT sum(o_price)
    FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'a') AS cn ON c_custkey = o_custkey
    SETTINGS query_plan_merge_expression_into_join = 1
)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT sum(o_price)
FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'a') AS cn ON c_custkey = o_custkey
SETTINGS query_plan_merge_expression_into_join = 1;
SELECT sum(o_price)
FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'a') AS cn ON c_custkey = o_custkey
SETTINGS query_plan_merge_expression_into_join = 1, enable_join_runtime_filters = 0;

DROP TABLE l;
DROP TABLE r;
DROP TABLE orders;
DROP TABLE customer;
DROP TABLE nation;
