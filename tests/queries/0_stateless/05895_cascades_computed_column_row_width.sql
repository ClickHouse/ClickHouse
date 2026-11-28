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

-- A filter directly above a read. The width of the rows of the read comes from all its columns, before the filter is
-- applied, so a filter that computes `concat` from two columns emits rows as wide as a filter that passes the two
-- columns. The tables have real data and column statistics, without hints. The part of `t_row_width_measured` is
-- compact: the storage has one size for all its columns together, and most of it goes to the wide column `unused`,
-- so the size of `url` comes from the statistics.
DROP TABLE IF EXISTS t_row_width_keys;
DROP TABLE IF EXISTS t_row_width_measured;

CREATE TABLE t_row_width_keys (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
SET allow_suspicious_fixed_string_types = 1;
CREATE TABLE t_row_width_measured (k UInt64, url String, phone String, unused FixedString(4096)) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
SET materialize_statistics_on_insert = 1;
INSERT INTO t_row_width_keys SELECT number % 1000 FROM numbers(1000000);
INSERT INTO t_row_width_measured SELECT number, repeat('x', 8192), '25-989', '' FROM numbers(1000);

SET use_statistics = 1;
-- The condition stays in a filter step above the read.
SET optimize_move_to_prewhere = 0;

SELECT '-- concat is computed in the filter above the read: a shuffle join';
EXPLAIN SELECT count(), sum(length(d.c))
FROM t_row_width_keys AS f INNER JOIN (SELECT k, concat(url, phone) AS c FROM t_row_width_measured WHERE length(c) > 0) AS d ON f.k = d.k;

SELECT '-- the filter passes url and phone, an expression above it computes concat: the same join';
EXPLAIN SELECT count(), sum(length(d.c))
FROM t_row_width_keys AS f INNER JOIN (SELECT k, concat(url, phone) AS c FROM t_row_width_measured WHERE length(url) + length(phone) > 0) AS d ON f.k = d.k;

DROP TABLE t_row_width_keys;
DROP TABLE t_row_width_measured;
