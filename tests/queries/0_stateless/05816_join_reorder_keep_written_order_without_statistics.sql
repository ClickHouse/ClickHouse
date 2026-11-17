-- A join graph of more than two tables keeps its written order when a table is filtered on a column that is
-- neither in its primary key nor has statistics: https://github.com/ClickHouse/ClickHouse/issues/129254

SET use_hash_table_stats_for_join_reordering = 0, enable_join_runtime_filters = 0, query_plan_optimize_join_order_randomize = 0,
    max_bytes_ratio_before_external_join = 0, query_plan_optimize_join_order_limit = 10, query_plan_join_swap_table = 'auto',
    enable_join_transitive_predicates = 0, use_statistics = 1, min_marks_per_part_to_load_statistics = 0,
    optimize_functions_to_subcolumns = 1, explain_query_plan_default = 'legacy';

CREATE TABLE orders (o_orderkey Int32, o_custkey Int32, o_totalprice Decimal(15, 2))
ENGINE = MergeTree ORDER BY o_orderkey SETTINGS auto_statistics_types = '', index_granularity = 8192;
CREATE TABLE customer (c_custkey Int32, c_nationkey Int32)
ENGINE = MergeTree ORDER BY c_custkey SETTINGS auto_statistics_types = '', index_granularity = 128;
CREATE TABLE nation (n_nationkey Int32, n_name String)
ENGINE = MergeTree ORDER BY n_nationkey SETTINGS auto_statistics_types = '', index_granularity = 8192;

INSERT INTO orders SELECT number, number % 20000, number % 1000 FROM numbers(100000);
INSERT INTO customer SELECT number, 5 FROM numbers(1);
INSERT INTO customer SELECT number + 1, 6 FROM numbers(100);
INSERT INTO customer SELECT number + 1000, 7 FROM numbers(10000);
INSERT INTO nation VALUES (5, 'ETHIOPIA'), (6, 'FRANCE'), (7, 'GERMANY'), (100, 'UNKNOWN');

-- Statistics of one of the two parts only.
CREATE TABLE nation_partial (n_nationkey Int32, n_name String)
ENGINE = MergeTree ORDER BY n_nationkey SETTINGS auto_statistics_types = 'basic', index_granularity = 8192;
SYSTEM STOP MERGES nation_partial;
INSERT INTO nation_partial SETTINGS materialize_statistics_on_insert = 0 VALUES (5, 'ETHIOPIA'), (6, 'FRANCE');
INSERT INTO nation_partial SETTINGS materialize_statistics_on_insert = 1 VALUES (7, 'GERMANY'), (100, 'UNKNOWN');

CREATE TABLE nation_stats (n_nationkey Int32, n_name String)
ENGINE = MergeTree ORDER BY n_nationkey SETTINGS auto_statistics_types = 'basic', index_granularity = 8192;
INSERT INTO nation_stats SETTINGS materialize_statistics_on_insert = 1 VALUES (5, 'ETHIOPIA'), (6, 'FRANCE'), (7, 'GERMANY'), (100, 'UNKNOWN');

SELECT 'filter on a column without statistics';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey)
WHERE explain ILIKE '%Join: %';
SELECT count() FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'WAKANDA') AS cn ON c_custkey = o_custkey
SETTINGS log_comment = 'build_rows_empty';
SELECT count() FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY') AS cn ON c_custkey = o_custkey
SETTINGS log_comment = 'build_rows_one_nation';

SELECT 'filter on the primary key';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE o_orderkey < 50000)
WHERE explain ILIKE '%Join: %';

SELECT 'primary key and another column';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE c_custkey < 5000 AND c_nationkey = 7) AS cn ON c_custkey = o_custkey)
WHERE explain ILIKE '%Join: %';

SELECT 'sub-join reordered on its own';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders
    JOIN (SELECT c_custkey, c_nationkey FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY' LIMIT 100000) AS cn ON c_custkey = o_custkey
    JOIN nation AS n2 ON n2.n_nationkey = cn.c_nationkey)
WHERE explain ILIKE '%Join: %';

SELECT 'sub-join not reordered';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders
    JOIN (SELECT c_custkey, c_nationkey FROM customer ASOF JOIN (SELECT * FROM nation WHERE n_name = 'GERMANY') AS n
        ON c_nationkey = n.n_nationkey AND c_custkey >= n.n_nationkey) AS cn ON c_custkey = o_custkey
    JOIN nation AS n2 ON n2.n_nationkey = cn.c_nationkey)
WHERE explain ILIKE '%Join: %';

SELECT 'statistics of some parts';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation_partial ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY')
WHERE explain ILIKE '%Join: %';

SELECT 'aggregation without keys';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey
    JOIN (SELECT max(n_nationkey) AS k FROM nation WHERE n_name = 'GERMANY') AS m ON c_nationkey = m.k)
WHERE explain ILIKE '%Join: %';

SELECT 'filter above an aggregation';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey
    JOIN (SELECT n_nationkey, count() AS c FROM nation GROUP BY n_nationkey HAVING c > 0) AS h ON c_nationkey = h.n_nationkey)
WHERE explain ILIKE '%Join: %';

SELECT 'subcolumn of a column with statistics';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation_stats ON c_nationkey = n_nationkey WHERE n_name != '')
WHERE explain ILIKE '%Join: %';

SELECT 'subcolumn of a column without statistics';
SELECT replaceRegexpAll(trimLeft(explain), '\\[[^]]*\\]', '') FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE n_name != '')
WHERE explain ILIKE '%Join: %';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['JoinBuildTableRowCount'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('build_rows_empty', 'build_rows_one_nation') AND type = 'QueryFinish'
ORDER BY log_comment;
