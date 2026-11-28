-- A join above a RIGHT or FULL join must not be sharded by primary-key ranges on a key of its left table: the rows
-- that only the right table has carry the default value in the left columns, outside the range of their stream.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;
SET use_statistics = 0;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET join_use_nulls = 0;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS t_05865_l;
DROP TABLE IF EXISTS t_05865_r;
DROP TABLE IF EXISTS t_05865_d;

CREATE TABLE t_05865_l (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE t_05865_r (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;
CREATE TABLE t_05865_d (a UInt64, v UInt8 DEFAULT 1) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;

-- The 100 rows only in `r` have `l.a = 0`, which matches `d.a = 0`.
INSERT INTO t_05865_l SELECT number + 1 FROM numbers(1000);
INSERT INTO t_05865_r SELECT number + 901 FROM numbers(200);
INSERT INTO t_05865_d (a) SELECT number FROM numbers(1000);

SELECT 'full', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count(), sum(d.v) FROM t_05865_l AS l FULL JOIN t_05865_r AS r ON l.a = r.a LEFT JOIN t_05865_d AS d ON l.a = d.a);
SELECT count(), sum(d.v) FROM t_05865_l AS l FULL JOIN t_05865_r AS r ON l.a = r.a LEFT JOIN t_05865_d AS d ON l.a = d.a;

SELECT 'right', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count(), sum(d.v) FROM t_05865_l AS l RIGHT JOIN t_05865_r AS r ON l.a = r.a JOIN t_05865_d AS d ON l.a = d.a);
SELECT count(), sum(d.v) FROM t_05865_l AS l RIGHT JOIN t_05865_r AS r ON l.a = r.a JOIN t_05865_d AS d ON l.a = d.a;

SELECT 'right subquery', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count(), sum(d.v) FROM t_05865_d AS d JOIN (SELECT l.a AS la FROM t_05865_l AS l RIGHT JOIN t_05865_r AS r ON l.a = r.a) AS s ON d.a = s.la);
SELECT count(), sum(d.v) FROM t_05865_d AS d JOIN (SELECT l.a AS la FROM t_05865_l AS l RIGHT JOIN t_05865_r AS r ON l.a = r.a) AS s ON d.a = s.la;

-- `r.a + 0` is not the primary key, so the RIGHT or FULL join itself is not sharded.
SELECT 'full unsharded', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count(), sum(d.v) FROM t_05865_l AS l FULL JOIN t_05865_r AS r ON l.a = r.a + 0 LEFT JOIN t_05865_d AS d ON l.a = d.a);
SELECT count(), sum(d.v) FROM t_05865_l AS l FULL JOIN t_05865_r AS r ON l.a = r.a + 0 LEFT JOIN t_05865_d AS d ON l.a = d.a;

SELECT 'right unsharded', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count(), sum(d.v) FROM t_05865_l AS l RIGHT JOIN t_05865_r AS r ON l.a = r.a + 0 JOIN t_05865_d AS d ON l.a = d.a);
SELECT count(), sum(d.v) FROM t_05865_l AS l RIGHT JOIN t_05865_r AS r ON l.a = r.a + 0 JOIN t_05865_d AS d ON l.a = d.a;

-- A LEFT join keeps the shards of its left table.
SELECT 'left', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count(), sum(d.v) FROM t_05865_l AS l LEFT JOIN t_05865_r AS r ON l.a = r.a LEFT JOIN t_05865_d AS d ON l.a = d.a);
SELECT count(), sum(d.v) FROM t_05865_l AS l LEFT JOIN t_05865_r AS r ON l.a = r.a LEFT JOIN t_05865_d AS d ON l.a = d.a;

DROP TABLE t_05865_l;
DROP TABLE t_05865_r;
DROP TABLE t_05865_d;
