-- A join keyed on the right table of an inner join below it is sharded by primary-key ranges,
-- because every row of an inner join has equal keys on both sides. A LEFT join's right key is not
-- a shard key: its non-matched rows carry defaults.

SET query_plan_join_shard_by_pk_ranges = 1;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET optimize_read_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET enable_parallel_replicas = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_d;

CREATE TABLE t_a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
CREATE TABLE t_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
CREATE TABLE t_c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
CREATE TABLE t_d (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

INSERT INTO t_a SELECT number * 2, number % 100 FROM numbers(10000);
INSERT INTO t_a SELECT number * 2 + 1, number % 100 FROM numbers(10000);
INSERT INTO t_b SELECT number * 2, number % 10 FROM numbers(10000);
INSERT INTO t_c SELECT number, number % 7 FROM numbers(20000);
INSERT INTO t_d SELECT number % 15000, number % 3 FROM numbers(30000);

SET join_algorithm = 'full_sorting_merge';

SELECT 'fsm', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count() FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id JOIN t_d ON t_c.id = t_d.id);

SELECT count(), sum(t_a.v), sum(t_d.v) FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id JOIN t_d ON t_c.id = t_d.id
SETTINGS query_plan_join_shard_by_pk_ranges = 1;
SELECT count(), sum(t_a.v), sum(t_d.v) FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id JOIN t_d ON t_c.id = t_d.id
SETTINGS query_plan_join_shard_by_pk_ranges = 0;

SET join_algorithm = 'hash';

SELECT 'hash', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count() FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id);

SELECT count(), sum(t_a.v), sum(t_c.v) FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 1;
SELECT count(), sum(t_a.v), sum(t_c.v) FROM t_a JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 0;

SELECT 'left', countIf(explain LIKE '%Sharding:%') FROM (EXPLAIN actions = 1
    SELECT count() FROM t_a LEFT JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id);

SELECT count(), sum(t_a.v), sum(t_c.v) FROM t_a LEFT JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 1;
SELECT count(), sum(t_a.v), sum(t_c.v) FROM t_a LEFT JOIN t_b ON t_a.id = t_b.id JOIN t_c ON t_b.id = t_c.id
SETTINGS query_plan_join_shard_by_pk_ranges = 0;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_d;
