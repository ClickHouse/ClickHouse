-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(10, None); use_statistics=(1, 1); use_hash_table_stats_for_join_reordering=(0, 0); enable_join_runtime_filters=(0, 0); max_bytes_ratio_before_external_join=(0, 0); use_query_condition_cache=(0, 0)

-- A filtered table without statistics has no row estimate. Its join graph keeps the written order
-- of the joins instead of putting the large table on the build side.
-- https://github.com/ClickHouse/ClickHouse/issues/129254

-- Random estimates, the hash table statistics and the query condition cache would each give `nation` a row estimate.
SET collect_hash_table_stats_during_joins = 0;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;

CREATE TABLE orders (o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2)) ENGINE = MergeTree ORDER BY o_orderkey;
CREATE TABLE customer (c_custkey Int32, c_nationkey Int32) ENGINE = MergeTree ORDER BY c_custkey;
-- The statistics types are pinned so that `MATERIALIZE STATISTICS` below gives `nation` a row estimate.
CREATE TABLE nation (n_nationkey Int32, n_name String) ENGINE = MergeTree ORDER BY n_nationkey SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- Parts without statistics, as written before 26.8.
SET materialize_statistics_on_insert = 0;
INSERT INTO orders SELECT number, number % 10000, number % 1000 FROM numbers(100000);
INSERT INTO customer SELECT number, if(number < 100, 6, 7) FROM numbers(10000);
INSERT INTO nation VALUES (5, 'ETHIOPIA'), (6, 'FRANCE'), (7, 'GERMANY'), (100, 'UNKNOWN');

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05498 q9';

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05498 q6';

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05498 q9_greedy', query_plan_optimize_join_order_algorithm = 'greedy';

SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
FORMAT Null SETTINGS log_comment = '05498 q9_dpsize', query_plan_optimize_join_order_algorithm = 'dpsize';

SELECT count() FROM customer JOIN orders ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'FRANCE'
FORMAT Null SETTINGS log_comment = '05498 left_deep';

-- With statistics `nation` has a row estimate, and the joins are reordered.
ALTER TABLE nation MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;

SELECT count() FROM customer JOIN orders ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'FRANCE'
FORMAT Null SETTINGS log_comment = '05498 with_statistics';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    if(log_comment = '05498 with_statistics',
        if(build < 1000, 'reordered', format('written order: {}', build)),
        toString(build))
FROM
(
    SELECT log_comment, ProfileEvents['JoinBuildTableRowCount'] AS build
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05498 %'
)
ORDER BY log_comment;

DROP TABLE orders;
DROP TABLE customer;
DROP TABLE nation;
