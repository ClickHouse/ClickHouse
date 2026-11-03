-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- The tags are those of the other `UNIQUE KEY` tests, such as 04174.

-- A read of a `UNIQUE KEY` table drops the rows that a later insert replaced. The `uniq` statistic of the parts still
-- counts their values, so it does not count the keys of the build, and the build defers.

SET enable_unique_key = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET join_algorithm = 'parallel_hash';
SET hash_join_layout = 'parallel';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` build defer. This one is far
-- above what this build takes.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET enable_hash_join_deferred_build = 1;

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_probe SELECT number FROM numbers(1000);

-- The parts store 301000 distinct values of `v`, and the read returns 1000 of them.
CREATE TABLE t_uk (id UInt64, v UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);
INSERT INTO t_uk SELECT number % 100000, number FROM numbers(0, 100000);
INSERT INTO t_uk SELECT number % 100000, number FROM numbers(100000, 100000);
INSERT INTO t_uk SELECT number % 100000, number FROM numbers(200000, 100000);
INSERT INTO t_uk SELECT number % 100000, number % 1000 FROM numbers(300000, 100000);

SELECT count(), uniqExact(v) FROM t_uk;
SELECT count() FROM t_probe l INNER JOIN t_uk AS r ON l.k = r.v SETTINGS log_comment = '05490 unique_key';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['HashJoinPreallocatedElementsInHashTables'] >= 90000, ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05490 unique_key';
