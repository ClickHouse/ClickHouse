-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(10, None); use_statistics=(1, 1); use_hash_table_stats_for_join_reordering=(0, 0); max_bytes_ratio_before_external_join=(0, 0); use_query_condition_cache=(0, 0)

-- A filtered table without statistics has no row estimate. Its join graph keeps the written order
-- of the joins instead of putting the large table on the build side.
-- https://github.com/ClickHouse/ClickHouse/issues/129254

-- Random estimates, the hash table statistics and the query condition cache would each give `nation` a row estimate.
-- A join runtime filter would shrink the build side before `JoinBuildTableRowCount` counts it.
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS keys_log;

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

-- Past the graph size limit, `customer JOIN nation` is optimized on its own and enters the graph above it
-- as one relation without an estimate, so that graph keeps the written order too.
SELECT 'graph_boundary', count()
FROM
(
    EXPLAIN keep_logical_steps = 1
    SELECT count() FROM customer JOIN nation ON c_nationkey = n_nationkey JOIN orders ON o_custkey = c_custkey
    JOIN nation AS n2 ON n2.n_nationkey = c_nationkey WHERE nation.n_name = 'FRANCE'
    SETTINGS query_plan_optimize_join_order_limit = 3
)
WHERE explain ILIKE '%Join: customer%⋈ nation%⋈ orders%⋈ n2%';

-- `cn` is an `ASOF` join, optimized on its own; its filtered `nation` has no statistics, so the graph above it keeps the written order.
SELECT avg(o_totalprice) FROM orders
JOIN (SELECT * FROM nation ASOF JOIN customer ON n_nationkey = c_nationkey AND n_nationkey <= c_custkey WHERE n_name = 'FRANCE') AS cn
    ON c_custkey = o_custkey
JOIN nation AS n2 ON n2.n_nationkey = cn.c_nationkey
FORMAT Null SETTINGS log_comment = '05498 asof';

-- After a first run, the hash table statistics give the filtered `nation` inside the unknown `keys_log ⋈ nation`
-- (`Log` has no statistics) a row estimate, so the graph above it is reordered.
CREATE TABLE keys_log (l_key Int32) ENGINE = Log;
INSERT INTO keys_log SELECT number % 10 FROM numbers(100);

SELECT count() FROM keys_log JOIN nation ON l_key = n_nationkey JOIN customer ON c_custkey = l_key
JOIN nation AS n2 ON n2.n_nationkey = nation.n_nationkey WHERE nation.n_name = 'FRANCE'
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 3, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

SELECT 'cached_boundary', count()
FROM
(
    EXPLAIN keep_logical_steps = 1
    SELECT count() FROM keys_log JOIN nation ON l_key = n_nationkey JOIN customer ON c_custkey = l_key
    JOIN nation AS n2 ON n2.n_nationkey = nation.n_nationkey WHERE nation.n_name = 'FRANCE'
    SETTINGS query_plan_optimize_join_order_limit = 3, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1
)
WHERE explain ILIKE '%Join: keys_log%⋈ nation%⋈ n2%⋈ customer%';

-- The same when the hash table statistics give the row estimate to `cn`, a sub-join that contains the filtered `nation`.
SELECT count() FROM keys_log JOIN (SELECT * FROM nation ANY JOIN customer ON n_nationkey = c_nationkey WHERE n_name = 'FRANCE') AS cn
    ON l_key = cn.n_nationkey
JOIN customer AS c2 ON c2.c_custkey = l_key JOIN nation AS n2 ON n2.n_nationkey = cn.n_nationkey
FORMAT Null SETTINGS query_plan_optimize_join_order_limit = 3, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;

SELECT 'cached_composite_boundary', count()
FROM
(
    EXPLAIN keep_logical_steps = 1
    SELECT count() FROM keys_log JOIN (SELECT * FROM nation ANY JOIN customer ON n_nationkey = c_nationkey WHERE n_name = 'FRANCE') AS cn
        ON l_key = cn.n_nationkey
    JOIN customer AS c2 ON c2.c_custkey = l_key JOIN nation AS n2 ON n2.n_nationkey = cn.n_nationkey
    SETTINGS query_plan_optimize_join_order_limit = 3, collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1
)
WHERE explain ILIKE '%Join: keys_log%⋈ (cn)%⋈ n2%⋈ c2%';

-- With statistics `nation` has a row estimate, and the joins are reordered.
ALTER TABLE nation MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;

SELECT count() FROM customer JOIN orders ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'FRANCE'
FORMAT Null SETTINGS log_comment = '05498 with_statistics';

-- `nation JOIN n2` is optimized on its own below the graph limited by `customer`; its tables are estimated once,
-- so the statistics are loaded as often as with `query_plan_optimize_join_order_limit = 1`.
SELECT count() FROM nation JOIN nation AS n2 ON n2.n_nationkey = nation.n_nationkey JOIN customer ON c_nationkey = nation.n_nationkey
WHERE c_nationkey = 6
FORMAT Null SETTINGS log_comment = '05498-reuse sub_join limited';

SELECT count() FROM nation JOIN nation AS n2 ON n2.n_nationkey = nation.n_nationkey JOIN customer ON c_nationkey = nation.n_nationkey
WHERE c_nationkey = 6
FORMAT Null SETTINGS log_comment = '05498-reuse sub_join swap_only', query_plan_optimize_join_order_limit = 1;

-- The same for the joins below `x`, an `ASOF` join without an estimate, whose tables are looked at for missing statistics.
SELECT count() FROM orders
JOIN (SELECT * FROM (SELECT nation.n_nationkey AS k FROM nation JOIN nation AS n4 ON n4.n_nationkey = nation.n_nationkey) AS l
    ASOF JOIN (SELECT c_nationkey, c_custkey FROM customer JOIN nation AS n5 ON n5.n_nationkey = c_nationkey) AS r
    ON l.k = r.c_nationkey AND l.k <= r.c_custkey) AS x ON o_custkey = x.c_custkey
JOIN nation AS n2 ON n2.n_nationkey = x.k
FORMAT Null SETTINGS log_comment = '05498-reuse asof limited';

SELECT count() FROM orders
JOIN (SELECT * FROM (SELECT nation.n_nationkey AS k FROM nation JOIN nation AS n4 ON n4.n_nationkey = nation.n_nationkey) AS l
    ASOF JOIN (SELECT c_nationkey, c_custkey FROM customer JOIN nation AS n5 ON n5.n_nationkey = c_nationkey) AS r
    ON l.k = r.c_nationkey AND l.k <= r.c_custkey) AS x ON o_custkey = x.c_custkey
JOIN nation AS n2 ON n2.n_nationkey = x.k
FORMAT Null SETTINGS log_comment = '05498-reuse asof swap_only', query_plan_optimize_join_order_limit = 1;

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

SELECT 'statistics_loaded_once', shape, length(loads) = 2 AND loads[1] = loads[2] AND loads[1] > 0
FROM
(
    SELECT splitByChar(' ', log_comment)[2] AS shape, groupArray(ProfileEvents['LoadedStatistics']) AS loads
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05498-reuse %'
    GROUP BY shape
)
ORDER BY shape;

DROP TABLE orders;
DROP TABLE customer;
DROP TABLE nation;
DROP TABLE keys_log;
