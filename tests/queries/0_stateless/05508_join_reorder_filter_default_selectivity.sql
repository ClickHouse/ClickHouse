-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(10, None); use_statistics=(1, 1); use_hash_table_stats_for_join_reordering=(0, 0); max_bytes_ratio_before_external_join=(0, 0); use_query_condition_cache=(0, 0)

-- A filter that neither the index nor column statistics can estimate keeps 20% of the rows of the table,
-- so join reordering does not put the large table on the build side.
-- https://github.com/ClickHouse/ClickHouse/issues/129254

-- Random estimates, the hash table statistics and the query condition cache would each give the filtered tables another estimate.
-- A join runtime filter would shrink the build side before `JoinBuildTableRowCount` counts it.
-- A join sharded by primary key ranges collects no hash table statistics.
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS dim_big;
DROP TABLE IF EXISTS dim_small;
DROP TABLE IF EXISTS mid;
DROP TABLE IF EXISTS nation_log;
DROP TABLE IF EXISTS sorted_a;
DROP TABLE IF EXISTS sorted_b;
DROP TABLE IF EXISTS shrunk;

-- Merges would build statistics on the tables of several parts.
CREATE TABLE orders (o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2), o_flag UInt8)
    ENGINE = MergeTree ORDER BY o_orderkey SETTINGS auto_statistics_types = '';
CREATE TABLE customer (c_custkey Int32, c_nationkey Int32) ENGINE = MergeTree ORDER BY c_custkey;
CREATE TABLE nation (n_nationkey Int32, n_name String) ENGINE = MergeTree ORDER BY n_nationkey;
CREATE TABLE dim_big (k Int32, g Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE dim_small (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE mid (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE nation_log (n_nationkey Int32, n_name String) ENGINE = Log;
CREATE TABLE sorted_a (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE sorted_b (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shrunk (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';

-- Parts without statistics, as written before 26.8.
SET materialize_statistics_on_insert = 0;
INSERT INTO orders SELECT number, number % 10000, number % 1000, if(number % 100 = 0, 1, 0) FROM numbers(100000);
INSERT INTO customer SELECT number, if(number < 100, 6, 7) FROM numbers(10000);
INSERT INTO nation VALUES (5, 'ETHIOPIA'), (6, 'FRANCE'), (7, 'GERMANY'), (100, 'UNKNOWN');
INSERT INTO dim_big SELECT number, number % 200 FROM numbers(200000);
INSERT INTO dim_small SELECT number FROM numbers(2000);
INSERT INTO mid SELECT number FROM numbers(50000);
INSERT INTO nation_log SELECT * FROM nation;
INSERT INTO sorted_a SELECT number FROM numbers(1000);
INSERT INTO sorted_b SELECT number FROM numbers(1000);
INSERT INTO shrunk SELECT number FROM numbers(100000);

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

-- A `Log` table has no row count, so the join graph keeps its written order and `orders` stays the probe side.
SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation_log ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05508 log_dimension';

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

-- Inside the aggregation `orders` (assumed 20000) is the probe side, so only the sub-join `s` gets hash table statistics, measured
-- 99000 rows; its estimate comes from the assumed share, so the second run uses the measured rows and `mid` becomes the build side.
SELECT count() FROM mid JOIN
(
    SELECT o_orderkey AS k FROM orders JOIN customer ON o_custkey = c_custkey WHERE o_flag = 0 GROUP BY o_orderkey
) AS s ON s.k = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_grouped_join_1', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SELECT count() FROM mid JOIN
(
    SELECT o_orderkey AS k FROM orders JOIN customer ON o_custkey = c_custkey WHERE o_flag = 0 GROUP BY o_orderkey
) AS s ON s.k = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_grouped_join_2', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

-- The first run builds `shrunk` with 100000 rows; after it shrinks to 1000, its primary index count bounds the rows, so the stale
-- hash table statistics do not replace it and `shrunk` stays the build side.
SELECT count() FROM mid JOIN shrunk ON shrunk.k = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_primary_index_1', query_plan_join_swap_table = 'false', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
TRUNCATE TABLE shrunk;
INSERT INTO shrunk SELECT number FROM numbers(1000);
SELECT count() FROM mid JOIN shrunk ON shrunk.k = mid.k
FORMAT Null SETTINGS log_comment = '05508 cache_primary_index_2', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

-- The graph keeps its written order for `nation_log`, but `sorted_a JOIN sorted_b`, whose inputs have estimates, is still optimized
-- on its own: with 50M rows each (from the hints) read in order by the key, it is a merge join.
SELECT 'merge_join_sub_tree', count()
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM sorted_a JOIN sorted_b ON sorted_a.k = sorted_b.k JOIN nation_log ON sorted_b.k = nation_log.n_nationkey
    SETTINGS param__internal_join_table_stat_hints = '{"sorted_a": {"cardinality": 50000000, "distinct_keys": {"k": 50000000}}, "sorted_b": {"cardinality": 50000000, "distinct_keys": {"k": 50000000}}}',
        use_statistics = 0, query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_join_order_use_interesting_orders = 1,
        enable_join_transitive_predicates = 1, optimize_read_in_order = 1, max_rows_in_set_to_optimize_join = 0,
        query_plan_join_shard_by_pk_ranges = 1, optimize_sorting_by_input_stream_properties = 1
)
WHERE explain ILIKE '%Algorithm: FullSortingMergeJoin%';

-- Reordering `sorted_a JOIN sorted_b` on its own reuses the estimates of the graph above it, so the statistics of its tables are
-- loaded as often as when each join is optimized on its own.
SELECT count() FROM (EXPLAIN SELECT count() FROM sorted_a JOIN sorted_b ON sorted_a.k = sorted_b.k JOIN nation_log ON sorted_b.k = nation_log.n_nationkey)
FORMAT Null SETTINGS log_comment = '05508_stats sub_tree';
SELECT count() FROM (EXPLAIN SELECT count() FROM sorted_a JOIN sorted_b ON sorted_a.k = sorted_b.k JOIN nation_log ON sorted_b.k = nation_log.n_nationkey)
FORMAT Null SETTINGS log_comment = '05508_stats one_join_at_a_time', query_plan_optimize_join_order_limit = 1;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['JoinBuildTableRowCount'] AS build
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05508 %'
ORDER BY log_comment;

SELECT 'statistics_loaded_once', count() = 2 AND min(loaded) > 0 AND min(loaded) = max(loaded)
FROM
(
    SELECT ProfileEvents['LoadedStatistics'] AS loaded
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05508_stats %'
);

DROP TABLE orders;
DROP TABLE customer;
DROP TABLE nation;
DROP TABLE dim_big;
DROP TABLE dim_small;
DROP TABLE mid;
DROP TABLE nation_log;
DROP TABLE sorted_a;
DROP TABLE sorted_b;
DROP TABLE shrunk;
