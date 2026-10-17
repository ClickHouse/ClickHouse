-- GROUP BY key expressions over one column that keep its type must not share hash-table size statistics: only
-- the expression step below the aggregation tells them apart. The second run of a query preallocates what the
-- first one recorded, while the first run of a different expression must preallocate nothing.
--
-- Each run is checked through the `AggregationPreallocatedElementsInHashTables` profile event. The group counts
-- stay above the 500e3 lower bound under which `getSizeHint` does not preallocate.

SET enable_analyzer = 1;
SET max_threads = 1;
-- Checks the aggregation of this node; with parallel replicas it would run on the replicas.
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_aggregation = 1;
SET max_size_to_preallocate_for_aggregation = 1000000000000;
-- Spilling the aggregation stops the statistics collection.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS t_05401;
DROP TABLE IF EXISTS t2_05401;
DROP TABLE IF EXISTS s_05401;
DROP TABLE IF EXISTS u_05401;
CREATE TABLE t_05401 (a UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05401 SELECT number, number FROM numbers(1200000);
-- The subquery shape reads its own table, so it cannot reuse an entry of the queries above.
CREATE TABLE t2_05401 (a UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2_05401 SELECT number FROM numbers(1200000);
CREATE TABLE u_05401 (s String, d DateTime) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO u_05401 SELECT toString(number), toDateTime(number) FROM numbers(1200000);

SELECT a + 1 AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_plus_1';
SELECT a + 1 AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_plus_2';
SELECT intDiv(a, 2) AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_div2_1';
SELECT intDiv(a, 2) AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_div2_2';
-- Differs from the query above only in a constant.
SELECT intDiv(a, 1000000) AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_div1m_1';

SELECT n + 1 AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_nullable_plus_1';
SELECT n + 1 AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_nullable_plus_2';
SELECT intDiv(n, 2) AS k FROM t_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_nullable_div2_1';

-- Differ only in a string literal, in a step that keeps the column type and in one that changes it.
SELECT trimBoth(s, 'x') AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_trim_x_1';
SELECT trimBoth(s, 'x') AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_trim_x_2';
SELECT trimBoth(s, '0123456789') AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_trim_digits_1';
SELECT formatDateTime(d, '%F %T') AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_format_full_1';
SELECT formatDateTime(d, '%F %T') AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_format_full_2';
SELECT formatDateTime(d, '%Y') AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_format_year_1';
-- An accepted collision: a value over 1 KiB is left out of the key, as a large folded constant is, so these two
-- share an entry.
SELECT trimBoth(s, repeat('x', 2000)) AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_long_x_1';
SELECT trimBoth(s, repeat('y', 2000)) AS k FROM u_05401 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_string_long_y_1';

-- Through a subquery the aggregation key is named `__table1.k` whatever it computes.
SELECT k FROM (SELECT a + 1 AS k FROM t2_05401) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_subquery_plus_1';
SELECT k FROM (SELECT a + 1 AS k FROM t2_05401) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_subquery_plus_2';
SELECT k FROM (SELECT intDiv(a, 2) AS k FROM t2_05401) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_subquery_div2_1';
SELECT k FROM (SELECT intDiv(a, 2) AS k FROM t2_05401) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05401_subquery_div2_2';

-- Automatic parallel replicas collects its statistics only when the single-replica and the parallel-replicas plan
-- builds key the boundary alike. With expression merging the projection absorbs the rename below it in one build
-- and not in the other, so what the projection computes must hash the same either way.
CREATE TABLE s_05401 (a String, b String) ENGINE = MergeTree ORDER BY b;
INSERT INTO s_05401 SELECT toString(number), toString(number) FROM numbers(1e6);
SELECT repeat(a, 100) AS a, b FROM s_05401 FORMAT Null SETTINGS
    enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, query_plan_merge_expressions = 1, max_threads = 4,
    log_comment = 'q05401_autopr_projection';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AggregationPreallocatedElementsInHashTables']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment LIKE 'q05401\_%' AND log_comment != 'q05401_autopr_projection'
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS statistics_collected
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment = 'q05401_autopr_projection';

DROP TABLE t_05401;
DROP TABLE t2_05401;
DROP TABLE s_05401;
DROP TABLE u_05401;
