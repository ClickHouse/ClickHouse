-- `query_plan_join_order_distribution`: DPsub orders the joins by the data that the distributed plan of Cascades moves
-- between nodes. The tables hold one row each; the statistics hints describe TPC-H at scale 100.

DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

CREATE TABLE nation (
    n_nationkey Int32, n_name String, n_regionkey Int32, n_comment String
) ENGINE = MergeTree() ORDER BY n_nationkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;
CREATE TABLE supplier (
    s_suppkey Int32, s_name String, s_address String, s_nationkey Int32,
    s_phone String, s_acctbal Decimal(15,2), s_comment String
) ENGINE = MergeTree() ORDER BY s_suppkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;
CREATE TABLE customer (
    c_custkey Int32, c_name String, c_address String, c_nationkey Int32,
    c_phone String, c_acctbal Decimal(15,2), c_mktsegment String, c_comment String
) ENGINE = MergeTree() ORDER BY c_custkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;
CREATE TABLE orders (
    o_orderkey Int32, o_custkey Int32, o_orderstatus String,
    o_totalprice Decimal(15,2), o_orderdate Date, o_orderpriority String,
    o_clerk String, o_shippriority Int32, o_comment String
) ENGINE = MergeTree() ORDER BY o_orderkey
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;
CREATE TABLE lineitem (
    l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
    l_quantity Decimal(15,2), l_extendedprice Decimal(15,2), l_discount Decimal(15,2),
    l_tax Decimal(15,2), l_returnflag String, l_linestatus String,
    l_shipdate Date, l_commitdate Date, l_receiptdate Date,
    l_shipinstruct String, l_shipmode String, l_comment String
) ENGINE = MergeTree() ORDER BY (l_orderkey, l_linenumber)
  SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 10737418240;

INSERT INTO nation   VALUES (1, 'FRANCE', 1, '');
INSERT INTO supplier VALUES (1, 'A', 'A', 1, '0', 1.0, '');
INSERT INTO customer VALUES (1, 'A', 'A', 1, '0', 1.0, 'B', '');
INSERT INTO orders   VALUES (1, 1, 'O', 1.0, '1995-01-01', '1-URGENT', 'C1', 0, '');
INSERT INTO lineitem VALUES (1, 1, 1, 1, 1.0, 2.0, 0.0, 0.0, 'N', 'O', '1995-02-01', '1995-01-15', '1995-02-05', 'DELIVER IN PERSON', 'SHIP', '');

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
-- The distribution-aware search runs only with the default `join_algorithm`.
SET join_algorithm = 'direct,hash,ie_join';
SET enable_join_runtime_filters = 0;
SET allow_statistic_optimize = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_transitive_predicates = 1;
-- The test profile of CI sets these limits, which keep aggregations, distinct and joins on one node.
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;
SET optimize_move_to_prewhere = 1;
SET use_join_disjunctions_push_down = 1;
SET send_logs_level = 'error';

SET param__internal_cascades_cluster_node_count = 20;
SET param__internal_cascades_cost_config = '{"work_weight":1,"exchange_fixed_overhead":3000,"network_weight":1,"sequential_weight":32}';
-- TPC-H Q07 at scale 100, after its filters.
SET param__internal_join_table_stat_hints = '{
    "supplier": { "cardinality": 1000000,   "avg_row_bytes": 8,  "distinct_keys": { "s_suppkey": 1000000,   "s_nationkey": 25 } },
    "lineitem": { "cardinality": 183000000, "avg_row_bytes": 26, "distinct_keys": { "l_orderkey": 150000000, "l_suppkey": 1000000, "l_shipdate": 730 } },
    "orders":   { "cardinality": 150000000, "avg_row_bytes": 8,  "distinct_keys": { "o_orderkey": 150000000, "o_custkey": 15000000 } },
    "customer": { "cardinality": 15000000,  "avg_row_bytes": 7,  "distinct_keys": { "c_custkey": 15000000,   "c_nationkey": 25 } },
    "nation":   { "cardinality": 1,         "avg_row_bytes": 68, "distinct_keys": { "n_nationkey": 1,         "n_name": 1 } }
}';

SELECT '-- Q07, join order by rows';
EXPLAIN
SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, toYear(l_shipdate) AS l_year, sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year
SETTINGS query_plan_join_order_distribution = 0;

SELECT '-- Q07, join order by the data moved between nodes';
EXPLAIN
SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, toYear(l_shipdate) AS l_year, sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year
SETTINGS query_plan_join_order_distribution = 1;

SELECT '-- The same rows in both join orders';
SELECT count(), sum(l_extendedprice)
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
SETTINGS query_plan_join_order_distribution = 0;
SELECT count(), sum(l_extendedprice)
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
SETTINGS query_plan_join_order_distribution = 1;

-- With an explicit `join_algorithm` Cascades may not distribute the joins: the setting then keeps the order by rows.
SELECT '-- No effect with an explicit join_algorithm';
SELECT (SELECT groupArray(explain) FROM (EXPLAIN
        SELECT count() FROM supplier, lineitem, orders, customer, nation n1, nation n2
        WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
        SETTINGS query_plan_join_order_distribution = 1, join_algorithm = 'full_sorting_merge'))
    = (SELECT groupArray(explain) FROM (EXPLAIN
        SELECT count() FROM supplier, lineitem, orders, customer, nation n1, nation n2
        WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
        SETTINGS query_plan_join_order_distribution = 0, join_algorithm = 'full_sorting_merge'));

-- A `UNION ALL` input has no row estimate of its own: the search costs it with the rows of its inputs. With one row
-- instead, joining the whole `u_customer` to it on one node would look cheaper than broadcasting the small join result.
DROP TABLE IF EXISTS u_sales1;
DROP TABLE IF EXISTS u_sales2;
DROP TABLE IF EXISTS u_customer;
DROP TABLE IF EXISTS u_date;
DROP TABLE IF EXISTS u_item;
CREATE TABLE u_sales1 (s_customer Int32, s_date Int32, s_item Int32) ENGINE = MergeTree ORDER BY s_item SETTINGS auto_statistics_types = '';
CREATE TABLE u_sales2 (s_customer Int32, s_date Int32, s_item Int32) ENGINE = MergeTree ORDER BY s_item SETTINGS auto_statistics_types = '';
CREATE TABLE u_customer (c_customer Int32, c_address Int32) ENGINE = MergeTree ORDER BY c_customer SETTINGS auto_statistics_types = '';
CREATE TABLE u_date (d_date Int32, d_month Int32) ENGINE = MergeTree ORDER BY d_date SETTINGS auto_statistics_types = '';
CREATE TABLE u_item (i_item Int32, i_category String) ENGINE = MergeTree ORDER BY i_item SETTINGS auto_statistics_types = '';
INSERT INTO u_sales1 VALUES (1, 1, 1);
INSERT INTO u_sales2 VALUES (1, 1, 1);
INSERT INTO u_customer VALUES (1, 1);
INSERT INTO u_date VALUES (1, 12);
INSERT INTO u_item VALUES (1, 'Women');

SET param__internal_cascades_cluster_node_count = 8;
SET param__internal_join_table_stat_hints = '{
    "u_sales1":   { "cardinality": 1441548, "avg_row_bytes": 12, "distinct_keys": { "s_customer": 100000, "s_date": 1800, "s_item": 18000 } },
    "u_sales2":   { "cardinality": 719384,  "avg_row_bytes": 12, "distinct_keys": { "s_customer": 100000, "s_date": 1800, "s_item": 18000 } },
    "u_customer": { "cardinality": 100000,  "avg_row_bytes": 8,  "distinct_keys": { "c_customer": 100000, "c_address": 50000 } },
    "u_date":     { "cardinality": 30,      "avg_row_bytes": 8,  "distinct_keys": { "d_date": 30 } },
    "u_item":     { "cardinality": 16,      "avg_row_bytes": 12, "distinct_keys": { "i_item": 16 } }
}';

SELECT '-- UNION ALL input';
EXPLAIN
SELECT DISTINCT c_customer, c_address
FROM (SELECT s_customer, s_date, s_item FROM u_sales1 UNION ALL SELECT s_customer, s_date, s_item FROM u_sales2) AS sales, u_customer, u_date, u_item
WHERE s_customer = c_customer AND s_date = d_date AND s_item = i_item AND d_month = 12 AND i_category = 'Women'
SETTINGS query_plan_join_order_distribution = 1;

SELECT DISTINCT c_customer, c_address
FROM (SELECT s_customer, s_date, s_item FROM u_sales1 UNION ALL SELECT s_customer, s_date, s_item FROM u_sales2) AS sales, u_customer, u_date, u_item
WHERE s_customer = c_customer AND s_date = d_date AND s_item = i_item AND d_month = 12 AND i_category = 'Women'
SETTINGS query_plan_join_order_distribution = 1;

-- A join without a row estimate that an earlier search planned enters the next join graph with the rows it was costed
-- with. With one row instead, joining it to `m_dim` first would look cheaper than broadcasting the join of the dimensions.
-- The `HAVING` over an aggregation of a join leaves the inner joins without an estimate, and the limit of the join order
-- search puts the last two joins into a graph of their own.
DROP TABLE IF EXISTS m_fact;
DROP TABLE IF EXISTS m_events;
DROP TABLE IF EXISTS m_users;
DROP TABLE IF EXISTS m_dim;
DROP TABLE IF EXISTS m_type;
CREATE TABLE m_fact (f_key Int32, f_dim Int32) ENGINE = MergeTree ORDER BY f_key SETTINGS auto_statistics_types = '';
CREATE TABLE m_events (e_key Int32, e_user Int32) ENGINE = MergeTree ORDER BY e_key SETTINGS auto_statistics_types = '';
CREATE TABLE m_users (u_user Int32, u_score Int32) ENGINE = MergeTree ORDER BY u_user SETTINGS auto_statistics_types = '';
CREATE TABLE m_dim (d_dim Int32, d_type Int32, d_name String) ENGINE = MergeTree ORDER BY d_dim SETTINGS auto_statistics_types = '';
CREATE TABLE m_type (t_type Int32, t_name String) ENGINE = MergeTree ORDER BY t_type SETTINGS auto_statistics_types = '';
INSERT INTO m_fact VALUES (1, 1);
INSERT INTO m_events VALUES (1, 1), (1, 1);
INSERT INTO m_users VALUES (1, 5);
INSERT INTO m_dim VALUES (1, 1, 'a');
INSERT INTO m_type VALUES (1, 'x');

SET param__internal_join_table_stat_hints = '{
    "m_fact":   { "cardinality": 20000000, "avg_row_bytes": 8,  "distinct_keys": { "f_key": 5000000, "f_dim": 2000000 } },
    "m_events": { "cardinality": 30000000, "avg_row_bytes": 8,  "distinct_keys": { "e_key": 5000000, "e_user": 1000000 } },
    "m_users":  { "cardinality": 1000000,  "avg_row_bytes": 8,  "distinct_keys": { "u_user": 1000000 } },
    "m_dim":    { "cardinality": 2000000,  "avg_row_bytes": 40, "distinct_keys": { "d_dim": 2000000, "d_type": 100000 } },
    "m_type":   { "cardinality": 5000,     "avg_row_bytes": 8,  "distinct_keys": { "t_type": 5000 } }
}';

SELECT '-- Join from an earlier join graph';
EXPLAIN
SELECT count(), sum(length(d_name))
FROM (SELECT f_dim FROM m_fact JOIN (SELECT e_key FROM m_events JOIN m_users ON e_user = u_user GROUP BY e_key HAVING sum(u_score) > 2 * count()) AS repeated ON f_key = e_key) AS x
JOIN m_dim ON f_dim = d_dim
JOIN m_type ON d_type = t_type
SETTINGS query_plan_join_order_distribution = 1, query_plan_optimize_join_order_limit = 3;

SELECT count(), sum(length(d_name))
FROM (SELECT f_dim FROM m_fact JOIN (SELECT e_key FROM m_events JOIN m_users ON e_user = u_user GROUP BY e_key HAVING sum(u_score) > 2 * count()) AS repeated ON f_key = e_key) AS x
JOIN m_dim ON f_dim = d_dim
JOIN m_type ON d_type = t_type
SETTINGS query_plan_join_order_distribution = 1, query_plan_optimize_join_order_limit = 3;

DROP TABLE nation;
DROP TABLE supplier;
DROP TABLE customer;
DROP TABLE orders;
DROP TABLE lineitem;
DROP TABLE u_sales1;
DROP TABLE u_sales2;
DROP TABLE u_customer;
DROP TABLE u_date;
DROP TABLE u_item;
DROP TABLE m_fact;
DROP TABLE m_events;
DROP TABLE m_users;
DROP TABLE m_dim;
DROP TABLE m_type;
