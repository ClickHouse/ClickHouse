-- Tags: shard
-- Shards limit the groups of `GROUP BY ... LIMIT` without aggregate functions,
-- but they do not get `group_by_overflow_mode = 'any'` for that.

SET optimize_trivial_group_by_limit_query = 1, max_block_size = 10;

SELECT number FROM numbers(1000) GROUP BY number LIMIT 5 FORMAT Null SETTINGS log_comment = 'local', max_threads = 1;
SELECT (SELECT number FROM numbers(1000) GROUP BY number LIMIT 1) FORMAT Null SETTINGS log_comment = 'scalar subquery';
SELECT number FROM remote('127.0.0.2', numbers(1000)) GROUP BY number LIMIT 5 FORMAT Null SETTINGS log_comment = 'one shard';
SELECT * FROM (SELECT number FROM remote('127.0.0.2', numbers(1000)) GROUP BY number LIMIT 5) FORMAT Null SETTINGS log_comment = 'subquery';
SELECT number FROM remote('127.0.0.{2,3}', numbers(1000)) GROUP BY number LIMIT 5 OFFSET 2 FORMAT Null SETTINGS log_comment = 'two shards';
SELECT number FROM remote('127.0.0.{2,3}', numbers(1000)) GROUP BY number LIMIT 5 FORMAT Null SETTINGS log_comment = 'two shards, plan', serialize_query_plan = 1;
SELECT number FROM remote('127.0.0.{2,3}', numbers(1000)) GROUP BY number LIMIT 5 FORMAT Null SETTINGS log_comment = 'two shards, disabled', optimize_trivial_group_by_limit_query = 0;
SELECT number, count() FROM remote('127.0.0.{2,3}', numbers(1000)) GROUP BY number LIMIT 5 FORMAT Null SETTINGS log_comment = 'two shards, aggregate';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, countIf(ProfileEvents['OverflowAny'] > 0), countIf(Settings['group_by_overflow_mode'] = 'any')
FROM system.query_log
WHERE event_date >= yesterday() AND type != 'QueryStart'
    AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND Settings['max_block_size'] = '10' AND query LIKE '%LIMIT%' AND query NOT LIKE '%query_log%')
GROUP BY log_comment
ORDER BY log_comment
SETTINGS max_block_size = 65409;

-- An explicit `throw` disables the optimization on the shards too.
SELECT number FROM remote('127.0.0.{2,3}', numbers(1000)) GROUP BY number LIMIT 5 SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT number FROM view(SELECT number FROM remote('127.0.0.2', numbers(1000)) GROUP BY number LIMIT 5 SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw') GROUP BY number LIMIT 5; -- { serverError TOO_MANY_ROWS }

-- Such a query over a trivial view is pushed down to the shards, as without the optimization.
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS d;
CREATE TABLE d (number UInt64) ENGINE = Distributed(test_cluster_two_shards, system, numbers);
CREATE VIEW v AS SELECT number FROM d;
SET optimize_trivial_view_pushdown_to_distributed = 1, max_rows_to_group_by = 0, serialize_query_plan = 0, analyzer_inline_views = 0;
SELECT 'disabled', countIf(explain LIKE '%MergingAggregated%') FROM (EXPLAIN SELECT number FROM v GROUP BY number LIMIT 5 SETTINGS optimize_trivial_group_by_limit_query = 0);
SELECT 'enabled', countIf(explain LIKE '%MergingAggregated%') FROM (EXPLAIN SELECT number FROM v GROUP BY number LIMIT 5);
DROP VIEW v;
DROP TABLE d;

-- With `make_distributed_plan` there is no limit on groups, so a distributed plan can be made.
DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number % 100 FROM numbers(1000);
SELECT 'union' FROM t GROUP BY k LIMIT 1 UNION ALL SELECT 'union' FROM t GROUP BY k LIMIT 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_cascades_optimizer = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
DROP TABLE t;
