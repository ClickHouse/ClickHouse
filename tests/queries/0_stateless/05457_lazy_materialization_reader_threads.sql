-- Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
-- `PartsSplitter` fault injection splits the two-part main read into more streams.

-- The lazy read of `ORDER BY ... LIMIT` creates its readers at run time. When the main read already runs in several
-- threads (here one stream per part), the query is given `max_threads` threads for them; a one-stream main read stays
-- in one thread.
-- The join case uses a non-parallel hash join, which keeps the probe side's width.
DROP TABLE IF EXISTS t_lazy_threads;
CREATE TABLE t_lazy_threads (k UInt64, ts UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
SYSTEM STOP MERGES t_lazy_threads;
INSERT INTO t_lazy_threads SELECT number, number % 2000, repeat(toString(number), 20) FROM numbers(10000)
SETTINGS min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
INSERT INTO t_lazy_threads SELECT number, number % 2000, repeat(toString(number), 20) FROM numbers(10000, 10000)
SETTINGS min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
DROP TABLE IF EXISTS t_lazy_threads_dim;
CREATE TABLE t_lazy_threads_dim (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_lazy_threads_dim SELECT number, toString(number) FROM numbers(20000);
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lazy_threads' AND active;

SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_lazy_threads ORDER BY ts, k LIMIT 10
    SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1
)
WHERE explain LIKE '%LazyReadFromMergeTreeSource%';
SELECT count() FROM
(
    EXPLAIN PIPELINE SELECT * FROM t_lazy_threads WHERE k < 10000 ORDER BY ts, k LIMIT 10
    SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1
)
WHERE explain LIKE '%LazyReadFromMergeTreeSource%';
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM
(
    EXPLAIN SELECT a.k, a.s, d.v FROM t_lazy_threads AS a JOIN t_lazy_threads_dim AS d ON a.k = d.k ORDER BY a.ts, a.k LIMIT 10
    SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1,
        query_plan_lazy_materialization_for_join = 1, join_algorithm = 'hash', enable_join_runtime_filters = 0,
        query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0
);

SET opentelemetry_start_trace_probability = 1;
SELECT * FROM t_lazy_threads ORDER BY ts, k LIMIT 10 FORMAT Null
SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 0,
    query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1,
    log_comment = '05457 main read alone';
SELECT * FROM t_lazy_threads ORDER BY ts, k LIMIT 10 FORMAT Null
SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1,
    log_comment = '05457 lazy read after a two-stream main read';
SELECT * FROM t_lazy_threads WHERE k < 10000 ORDER BY ts, k LIMIT 10 FORMAT Null
SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1,
    log_comment = '05457 lazy read after a one-stream main read';
SELECT a.k, a.s, d.v FROM t_lazy_threads AS a JOIN t_lazy_threads_dim AS d ON a.k = d.k ORDER BY a.ts, a.k LIMIT 10 FORMAT Null
SETTINGS max_threads = 8, max_threads_min_free_memory_per_thread = 0, query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 10000, enable_parallel_replicas = 0, log_queries = 1,
    query_plan_lazy_materialization_for_join = 1, join_algorithm = 'hash', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0,
    log_comment = '05457 lazy read over a join after a two-stream main read';
SET opentelemetry_start_trace_probability = 0;
SYSTEM FLUSH LOGS query_log, opentelemetry_span_log;

CREATE TEMPORARY TABLE traced ENGINE = Memory AS
SELECT DISTINCT query_id, log_comment FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment LIKE '05457 %';
CREATE TEMPORARY TABLE traces ENGINE = Memory AS
SELECT trace_id, attribute['clickhouse.query_id'] AS query_id FROM system.opentelemetry_span_log
WHERE finish_date >= yesterday() AND operation_name = 'query' AND attribute['clickhouse.query_id'] IN (SELECT query_id FROM traced);
SELECT t.log_comment, max(toUInt64(s.attribute['clickhouse.thread_num']))
FROM
(
    SELECT trace_id, attribute FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday() AND operation_name = 'PipelineExecutor::execute()' AND trace_id IN (SELECT trace_id FROM traces)
) AS s
JOIN traces USING (trace_id)
JOIN traced AS t USING (query_id)
GROUP BY t.log_comment ORDER BY t.log_comment;

DROP TABLE t_lazy_threads;
DROP TABLE t_lazy_threads_dim;
