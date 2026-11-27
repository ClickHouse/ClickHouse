-- The cost-based distributed optimizer estimates the width of the rows that each step emits, and the cost of an
-- exchange is in proportion to it. The row width is the sum of the sizes of the output columns if each size is
-- known. A computed value of a variable size with no known size (the result of `concat`, for example) can be as
-- wide as the columns that it is computed from, so the row then keeps at least the width of the input row.
-- The test shows it by the join type: a broadcast sends the right side to each of the 8 nodes, so it is the
-- cheaper join only if the rows of the right side are narrow.

DROP TABLE IF EXISTS t_row_width_fact;
DROP TABLE IF EXISTS t_row_width_dim;

CREATE TABLE t_row_width_fact (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_row_width_dim (k UInt64, url String, phone String) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_row_width_fact VALUES (1);
INSERT INTO t_row_width_dim VALUES (1, 'https://a.example/p', '25-989');

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 8;
-- The tables stand in for large ones: 30 million rows, and 100 thousand rows with a `url` of 4 KB.
SET param__internal_join_table_stat_hints = '{"t_row_width_fact": {"cardinality": 30000000, "distinct_keys": {"k": 100000}}, "t_row_width_dim": {"cardinality": 100000, "distinct_keys": {"k": 100000}, "column_bytes": {"url": 4096, "phone": 15}}}';

SELECT '-- the size of the result of concat is not known, the rows stay as wide as the rows of the table: a shuffle join';
EXPLAIN SELECT count(), sum(length(d.c))
FROM t_row_width_fact AS f INNER JOIN (SELECT k, concat(url, phone) AS c FROM t_row_width_dim) AS d ON f.k = d.k;

SELECT '-- the size of each column is known, the rows are 16 bytes wide: a broadcast join';
EXPLAIN SELECT count(), sum(d.c)
FROM t_row_width_fact AS f INNER JOIN (SELECT k, cityHash64(url) AS c FROM t_row_width_dim) AS d ON f.k = d.k;

DROP TABLE t_row_width_fact;
DROP TABLE t_row_width_dim;
