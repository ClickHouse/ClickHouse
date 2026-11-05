-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(10, None); use_statistics=(1, 1); use_hash_table_stats_for_join_reordering=(0, 0); max_bytes_ratio_before_external_join=(0, 0); use_query_condition_cache=(0, 0)

-- A filter that neither the index nor column statistics can estimate keeps 20% of the rows of the table,
-- so join reordering does not put the large table on the build side.
-- https://github.com/ClickHouse/ClickHouse/issues/129254

-- Random estimates, the hash table statistics and the query condition cache would each give the filtered tables another estimate.
-- A join runtime filter would shrink the build side before `JoinBuildTableRowCount` counts it.
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS dim_big;
DROP TABLE IF EXISTS dim_small;
DROP TABLE IF EXISTS mid;

-- Merges would build statistics on the tables of several parts.
CREATE TABLE orders (o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2), o_flag UInt8)
    ENGINE = MergeTree ORDER BY o_orderkey SETTINGS auto_statistics_types = '';
CREATE TABLE customer (c_custkey Int32, c_nationkey Int32) ENGINE = MergeTree ORDER BY c_custkey;
CREATE TABLE nation (n_nationkey Int32, n_name String) ENGINE = MergeTree ORDER BY n_nationkey;
CREATE TABLE dim_big (k Int32, g Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE dim_small (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE mid (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';

-- Parts without statistics, as written before 26.8.
SET materialize_statistics_on_insert = 0;
INSERT INTO orders SELECT number, number % 10000, number % 1000, if(number % 100 = 0, 1, 0) FROM numbers(100000);
INSERT INTO customer SELECT number, if(number < 100, 6, 7) FROM numbers(10000);
INSERT INTO nation VALUES (5, 'ETHIOPIA'), (6, 'FRANCE'), (7, 'GERMANY'), (100, 'UNKNOWN');
INSERT INTO dim_big SELECT number, number % 200 FROM numbers(200000);
INSERT INTO dim_small SELECT number FROM numbers(2000);
INSERT INTO mid SELECT number FROM numbers(50000);

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05508 q9';

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05508 q6';

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05508 q9_greedy', query_plan_optimize_join_order_algorithm = 'greedy';

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05508 q9_dpsize', query_plan_optimize_join_order_algorithm = 'dpsize';

SELECT count() FROM customer JOIN orders ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'FRANCE'
FORMAT Null SETTINGS log_comment = '05508 left_deep';

-- 20% of `dim_big` is below `orders`, so the filtered `dim_big` is the build side; all its rows would not be.
SELECT count() FROM (SELECT * FROM dim_big WHERE g = 7) AS d JOIN orders ON o_orderkey = d.k
FORMAT Null SETTINGS log_comment = '05508 filtered_dim_join_fact';

SELECT 'default_estimate', count()
FROM
(
    EXPLAIN keep_logical_steps = 1
    SELECT count() FROM (SELECT * FROM dim_big WHERE g = 7) AS d JOIN orders ON o_orderkey = d.k
)
WHERE explain ILIKE '%Join:%d[no_stats~40000]%';

-- 20% of the 4 rows of `nation` truncates to 0; the estimate is raised to 1 row.
SELECT 'floor_estimate', count()
FROM
(
    EXPLAIN keep_logical_steps = 1
    SELECT avg(o_totalprice) FROM orders
    JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY') AS cn ON c_custkey = o_custkey
)
WHERE explain ILIKE '%nation[no_stats~1]%';

-- `o_flag = 0` keeps 99% of `orders`; 20% of it is still above `dim_small`, which stays the build side.
SELECT count() FROM (SELECT * FROM orders WHERE o_flag = 0) AS f JOIN dim_small ON f.o_orderkey = dim_small.k
FORMAT Null SETTINGS log_comment = '05508 unselective_filter_fact';

-- The filter references both sides of the `LEFT JOIN`, so the sub-join is optimized on its own and the filter stays above it.
SELECT count() FROM
(
    SELECT dim_small.k AS k FROM dim_small LEFT JOIN customer ON dim_small.k = c_custkey
    WHERE c_nationkey = 6 OR dim_small.k < 10
) AS s
JOIN orders ON o_orderkey = s.k
FORMAT Null SETTINGS log_comment = '05508 filtered_sub_join';

-- The hash table statistics of a first run measure the rows of a filtered input; the second run uses them instead of the assumed
-- 20%, as on master, where the input has no estimate. `o_flag = 0` keeps 99000 rows, assumed 20000, so `mid` becomes the build side.
SELECT count() FROM mid JOIN (SELECT * FROM orders WHERE o_flag = 0) AS f ON f.o_orderkey = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_read_1', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SELECT count() FROM mid JOIN (SELECT * FROM orders WHERE o_flag = 0) AS f ON f.o_orderkey = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_read_2', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

SELECT count() FROM mid JOIN (SELECT o_orderkey FROM orders WHERE o_flag = 0 GROUP BY o_orderkey) AS f ON f.o_orderkey = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_aggregation_1', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SELECT count() FROM mid JOIN (SELECT o_orderkey FROM orders WHERE o_flag = 0 GROUP BY o_orderkey) AS f ON f.o_orderkey = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_aggregation_2', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

-- The filter above the sub-join keeps all but 90 of its 200000 rows, assumed 40000.
SELECT count() FROM mid JOIN
(
    SELECT dim_big.k AS k FROM dim_big LEFT JOIN customer ON dim_big.k = c_custkey
    WHERE c_nationkey != 6 OR dim_big.k < 10
) AS s ON s.k = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_sub_join_1', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SELECT count() FROM mid JOIN
(
    SELECT dim_big.k AS k FROM dim_big LEFT JOIN customer ON dim_big.k = c_custkey
    WHERE c_nationkey != 6 OR dim_big.k < 10
) AS s ON s.k = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_sub_join_2', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['JoinBuildTableRowCount'] AS build
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05508 %'
ORDER BY log_comment;

DROP TABLE orders;
DROP TABLE customer;
DROP TABLE nation;
DROP TABLE dim_big;
DROP TABLE dim_small;
DROP TABLE mid;
