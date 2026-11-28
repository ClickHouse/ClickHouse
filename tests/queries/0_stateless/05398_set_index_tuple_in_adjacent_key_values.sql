-- A tuple IN over (x, t) with t bounded by the partition key, where a granule spans two adjacent x values.

DROP TABLE IF EXISTS t_set_adjacent;
CREATE TABLE t_set_adjacent (t DateTime('UTC'), x Int32)
ENGINE = MergeTree PARTITION BY toYYYYMM(t) ORDER BY (x, t)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_set_adjacent SELECT toDateTime('2000-01-01 00:00:00', 'UTC'), number FROM numbers(2);
INSERT INTO t_set_adjacent SELECT toDateTime('2020-01-01 00:00:00', 'UTC'), number FROM numbers(2);

SELECT count() FROM t_set_adjacent
WHERE (x, t) IN ((0, toDateTime('2020-01-01 00:00:00', 'UTC')), (1, toDateTime('2000-01-01 00:00:00', 'UTC')))
SETTINGS use_lightweight_primary_key_index_analysis = 1, use_partition_minmax_for_primary_key_pruning = 1;

SELECT count() FROM t_set_adjacent
WHERE (x, t) IN ((0, toDateTime('2020-01-01 00:00:00', 'UTC')), (1, toDateTime('2000-01-01 00:00:00', 'UTC')))
SETTINGS use_lightweight_primary_key_index_analysis = 0, use_partition_minmax_for_primary_key_pruning = 1;

SELECT count() FROM t_set_adjacent
WHERE (x, t) NOT IN ((0, toDateTime('2020-01-01 00:00:00', 'UTC')), (1, toDateTime('2000-01-01 00:00:00', 'UTC')))
SETTINGS use_lightweight_primary_key_index_analysis = 1, use_partition_minmax_for_primary_key_pruning = 1;

-- The lazy FINAL set build hits the same index analysis.
DROP TABLE IF EXISTS t_set_adjacent_final;
CREATE TABLE t_set_adjacent_final (t DateTime('UTC'), x Int32, s String)
ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(t) ORDER BY (x, t)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_set_adjacent_final SELECT toDateTime('2000-01-01 00:00:00', 'UTC'), number, '' FROM numbers(2);
INSERT INTO t_set_adjacent_final SELECT toDateTime('2000-01-01 00:00:00', 'UTC'), number, 'updated' FROM numbers(2);
OPTIMIZE TABLE t_set_adjacent_final FINAL;
INSERT INTO t_set_adjacent_final SELECT toDateTime('2020-01-01 00:00:00', 'UTC'), number, '' FROM numbers(2);
INSERT INTO t_set_adjacent_final SELECT toDateTime('2020-01-01 00:00:00', 'UTC'), number, 'updated' FROM numbers(2);

SELECT max(x) FROM t_set_adjacent_final FINAL WHERE s = 'updated'
SETTINGS query_plan_optimize_lazy_final = 1, use_partition_minmax_for_primary_key_pruning = 1;

-- A granule boundary equal to a String partition bound gives an empty half-open key range.
DROP TABLE IF EXISTS t_set_equal_bound;
CREATE TABLE t_set_equal_bound (x Int32, s String, z Int32)
ENGINE = MergeTree PARTITION BY (s, z) ORDER BY (x, s, z)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_set_equal_bound SELECT number * 5, 'a', 3 FROM numbers(2);
INSERT INTO t_set_equal_bound SELECT number * 5, 'a', 4 FROM numbers(2);

SELECT count() FROM t_set_equal_bound WHERE (x, s, z) IN ((0, 'a', 3), (0, 'a', 4))
SETTINGS use_lightweight_primary_key_index_analysis = 1, use_partition_minmax_for_primary_key_pruning = 1;

SELECT count() FROM t_set_equal_bound WHERE (x, s, z) IN ((0, 'a', 3), (0, 'a', 4))
SETTINGS use_lightweight_primary_key_index_analysis = 0, use_partition_minmax_for_primary_key_pruning = 1;

SELECT count() FROM t_set_equal_bound WHERE (x, s, z) IN ((5, 'a', 2), (5, 'a', 3))
SETTINGS use_lightweight_primary_key_index_analysis = 1, use_partition_minmax_for_primary_key_pruning = 1;

DROP TABLE t_set_adjacent;
DROP TABLE t_set_adjacent_final;
DROP TABLE t_set_equal_bound;
