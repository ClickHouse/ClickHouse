-- Results of `ORDER BY ... LIMIT BY ... LIMIT` with the per-stream outer limit (`query_plan_push_down_limit_below_limit_by`)
-- in the shapes where each sorted stream stops early: outer `OFFSET`, both sort directions, `LIMIT n BY` with n > 1,
-- `LIMIT BY` offset, buffered read in order, `FINAL`, parallel replicas and a distributed read. Issue https://github.com/ClickHouse/ClickHouse/issues/113110.

DROP TABLE IF EXISTS t_lbol;
DROP TABLE IF EXISTS t_lbol_final;

CREATE TABLE t_lbol (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY (k, v) SETTINGS index_granularity = 64;
SYSTEM STOP MERGES t_lbol;
-- Four parts with overlapping keys, so every key has rows in several streams.
INSERT INTO t_lbol SELECT number % 5000, number FROM numbers(20000);
INSERT INTO t_lbol SELECT number % 5000, number + 100000 FROM numbers(20000);
INSERT INTO t_lbol SELECT (number * 7) % 5000, number + 200000 FROM numbers(20000);
INSERT INTO t_lbol SELECT number % 300, number + 300000 FROM numbers(3000);

SET query_plan_push_limit_by_into_sort = 1, query_plan_push_down_limit_below_limit_by = 1, optimize_read_in_order = 1, max_block_size = 7;

SELECT 'desc, offset';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 BY k LIMIT 6 OFFSET 9;
SELECT 'asc, offset';
SELECT k, v FROM t_lbol ORDER BY k, v LIMIT 3 BY k LIMIT 5 OFFSET 11;
SELECT 'limit by length above outer limit';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 10 BY k LIMIT 4;
SELECT 'single thread';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 BY k LIMIT 5 OFFSET 2 SETTINGS max_threads = 1;
SELECT 'buffered read in order';
SELECT k, v FROM t_lbol WHERE v % 3 = 0 ORDER BY k DESC, v DESC LIMIT 2 BY k LIMIT 6 OFFSET 3 SETTINGS read_in_order_use_buffering = 1, max_threads = 4;
SELECT 'unbuffered read in order';
SELECT k, v FROM t_lbol WHERE v % 3 = 0 ORDER BY k DESC, v DESC LIMIT 2 BY k LIMIT 6 OFFSET 3 SETTINGS read_in_order_use_buffering = 0, max_threads = 4;
SELECT 'parallel replicas';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 BY k LIMIT 6 OFFSET 5
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT 'distributed';
SELECT k, v FROM remote('127.0.0.{1,2}', currentDatabase(), t_lbol) ORDER BY k DESC, v DESC LIMIT 3 BY k LIMIT 7 OFFSET 4;
SELECT 'same rows as without the outer limit';
SELECT
    (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 BY k LIMIT 50 OFFSET 30))
    = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 BY k LIMIT 50 OFFSET 30 WITH TIES));

SELECT 'limit by offset';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 OFFSET 1 BY k LIMIT 6 OFFSET 3;
SELECT 'limit by offset, asc';
SELECT k, v FROM t_lbol ORDER BY k, v LIMIT 3 OFFSET 2 BY k LIMIT 5 OFFSET 4;
SELECT 'limit by offset above the rows of a group in one part';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 OFFSET 5 BY k LIMIT 6;
SELECT 'limit by offset, parallel replicas';
SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 2 OFFSET 1 BY k LIMIT 6 OFFSET 5
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT 'limit by offset, distributed';
SELECT k, v FROM remote('127.0.0.{1,2}', currentDatabase(), t_lbol) ORDER BY k DESC, v DESC LIMIT 2 OFFSET 1 BY k LIMIT 7 OFFSET 4;

-- `LIMIT n OFFSET m BY k LIMIT 7 OFFSET o` for every combination, against `WITH TIES`, which does not push the outer limit down
-- and returns the same rows because `(k, v)` is unique. A `SETTINGS` clause of a subquery does not change its plan.
SELECT 'desc, n = 1, m = 0, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'desc, n = 1, m = 0, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'desc, n = 1, m = 2, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'desc, n = 1, m = 2, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'desc, n = 1, m = 6, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'desc, n = 1, m = 6, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'desc, n = 3, m = 0, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'desc, n = 3, m = 0, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'desc, n = 3, m = 2, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'desc, n = 3, m = 2, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'desc, n = 3, m = 6, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'desc, n = 3, m = 6, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k DESC, v DESC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'asc, n = 1, m = 0, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'asc, n = 1, m = 0, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 0 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'asc, n = 1, m = 2, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'asc, n = 1, m = 2, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 2 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'asc, n = 1, m = 6, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'asc, n = 1, m = 6, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 1 OFFSET 6 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'asc, n = 3, m = 0, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'asc, n = 3, m = 0, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 0 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'asc, n = 3, m = 2, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'asc, n = 3, m = 2, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 2 BY k LIMIT 7 OFFSET 5 WITH TIES));
SELECT 'asc, n = 3, m = 6, offset = 0', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 0)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 0 WITH TIES));
SELECT 'asc, n = 3, m = 6, offset = 5', (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 5)) = (SELECT groupArray((k, v)) FROM (SELECT k, v FROM t_lbol ORDER BY k ASC, v ASC LIMIT 3 OFFSET 6 BY k LIMIT 7 OFFSET 5 WITH TIES));

-- `FINAL`: the replaced row of every key must not come back.
CREATE TABLE t_lbol_final (k UInt32, v UInt32, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 64;
SYSTEM STOP MERGES t_lbol_final;
INSERT INTO t_lbol_final SELECT number, number, 1 FROM numbers(5000);
INSERT INTO t_lbol_final SELECT number, number + 1000000, 2 FROM numbers(5000);
SELECT 'final';
SELECT k, v FROM t_lbol_final FINAL ORDER BY k DESC LIMIT 1 BY k LIMIT 4 OFFSET 2;

DROP TABLE t_lbol;
DROP TABLE t_lbol_final;
