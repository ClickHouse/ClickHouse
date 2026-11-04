-- Tags: shard

-- A `GROUP BY` over fixed-width keys and one `String` key gives the same result with the methods of such keys
-- (`keys32_packed_string`, `keys64_packed_string`, `keys128_packed_string`) as with the serialized method, on every
-- path of the ordinary aggregator. The strings are empty, made of zero bytes, kept inline or stored out of line,
-- around both inline limits: 11 bytes, and 15 bytes after a fixed-width part of at most 4 bytes. Each comparison
-- runs the path under test with the method on in one subquery and the plain path with the method off in the other,
-- so the profile events of the query show that the path under test ran.

SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 0;
SET optimize_aggregation_in_order = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_parallel_single_level_merge = 0;
SET enable_software_prefetch_in_aggregation = 1;
SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET query_plan_aggregation_bucket_top_k = 1;
-- The profile of the test server limits the number of groups, and an aggregation with such a limit keeps no top-K
-- heap. The distributed-plan suite serializes every plan, which the top-K plan passes refuse.
SET max_rows_to_group_by = 0;
SET serialize_query_plan = 0;
SET log_queries = 1;
SET log_profile_events = 1;

DROP VIEW IF EXISTS v_05481;
DROP VIEW IF EXISTS v_05481_preallocated;
DROP TABLE IF EXISTS t_05481_sorted;

-- `a` and `d` are 0 for some rows with an empty string: the key with all bytes zero.
CREATE VIEW v_05481 AS
SELECT
    toUInt32(intDiv(number, 3) % 4000) AS a,
    toUInt8(number % 3) AS b,
    toUInt16(number % 5) AS c,
    intDiv(number, 3) % 4000 * 1000003 AS d,
    multiIf(
        number % 29 = 0, '',
        number % 29 = 1, repeat('\0', 1 + number % 3),
        number % 29 = 2, repeat('y', 40 + number % 50),
        substring(concat(hex(cityHash64(intDiv(number, 7))), hex(cityHash64(intDiv(number, 11)))), 1, number % 29 - 2)) AS s
FROM numbers_mt(120000);

CREATE TABLE t_05481_sorted (c UInt16, a UInt32, b UInt8, s String, d UInt64) ENGINE = MergeTree ORDER BY (c, a)
SETTINGS index_granularity = 1024;
INSERT INTO t_05481_sorted SELECT c, a, b, s, d FROM v_05481;

-- 700 thousand keys, 200 thousand of them with an empty string, so that the hash table statistics give a size hint and
-- both sub-tables of the hash table get a part of it.
CREATE VIEW v_05481_preallocated AS
SELECT toUInt32(intDiv(number, 2) % 200000) AS a, if(number % 2 = 0, '', toString(number)) AS s FROM numbers_mt(1000000);

-- The method packs the fixed-width keys longest first and puts the string last, and the result restores the order of
-- the columns.
SELECT 'keys32, string first, fixed-width part of 3 bytes',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT s, b, c, count(), sum(d) FROM v_05481 GROUP BY s, b, c)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT s, b, c, count(), sum(d) FROM v_05481 GROUP BY s, b, c)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_string_first';

SELECT 'keys32, two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM v_05481 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM v_05481 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_two_level';

SELECT 'keys128, two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), min(c) FROM v_05481 GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), min(c) FROM v_05481 GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys128_two_level';

-- The merge of the written blocks reads the keys back with the serialized method.
SELECT 'keys32, external aggregation',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), uniqExact(d) FROM v_05481 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            max_bytes_before_external_group_by = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), uniqExact(d) FROM v_05481 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_external';

-- The single-level tables of the threads are merged in hash partitions, sub-table by sub-table.
SELECT 'keys32, partitioned single-level merge',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), uniqExact(d) FROM v_05481 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_parallel_single_level_merge = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), uniqExact(d) FROM v_05481 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_partitioned_merge';

-- Without the adaptive aggregator, a repeated query preallocates its tables by the hash table statistics of its earlier
-- runs. The first query of each pair records the statistics that the second one preallocates by.
SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05481_preallocated GROUP BY a, s)
SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, collect_hash_table_stats_during_aggregation = 1
FORMAT Null;

SELECT 'keys32, preallocated',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05481_preallocated GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, collect_hash_table_stats_during_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05481_preallocated GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_preallocated';

-- The statistics also show that the tables go two-level, so the second query starts with two-level tables.
SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05481_preallocated GROUP BY a, s)
SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, collect_hash_table_stats_during_aggregation = 1,
    group_by_two_level_threshold = 100000
FORMAT Null;

SELECT 'keys32, preallocated two-level',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05481_preallocated GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, collect_hash_table_stats_during_aggregation = 1,
            group_by_two_level_threshold = 100000)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05481_preallocated GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_preallocated_two_level';

-- `ROLLUP` merges the aggregated blocks again with the serialized method.
SELECT 'keys64, ROLLUP',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, count(), max(a) FROM v_05481 GROUP BY ROLLUP(d, s))
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, count(), max(a) FROM v_05481 GROUP BY ROLLUP(d, s))
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys64_rollup';

-- Aggregation in order hashes every run of equal values of the sorting key prefix with a state of its own. Such a state
-- packs the fixed-width values of a run of 24000 rows in a batch, and those of a run of a few rows one by one.
SELECT 'keys32, aggregation in order, long runs',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, b, s, count(), sum(d) FROM t_05481_sorted GROUP BY c, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, optimize_aggregation_in_order = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, b, s, count(), sum(d) FROM t_05481_sorted GROUP BY c, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys32_in_order_long_runs';

SELECT 'keys64, aggregation in order, short runs',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, a, s, count(), sum(d) FROM t_05481_sorted GROUP BY c, a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, optimize_aggregation_in_order = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, a, s, count(), sum(d) FROM t_05481_sorted GROUP BY c, a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05481_keys64_in_order_short_runs';

-- The bounded heap of `ORDER BY` keys `LIMIT` keeps copies of the keys it admits.
SELECT 'keys32, ORDER BY keys LIMIT',
    (SELECT groupArray(tuple(*)) FROM (SELECT a, s, count() FROM v_05481 GROUP BY a, s ORDER BY a DESC, s LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT a, s, count() FROM v_05481 GROUP BY a, s ORDER BY a DESC, s LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_group_by_top_k_optimization = 0)
SETTINGS log_comment = '05481_keys32_top_k_heap';

-- Groups with equal counts can take each other's place, so only the counts are compared.
SELECT 'keys64, ORDER BY count() LIMIT',
    (SELECT arraySort(groupArray(cnt)) FROM (SELECT d, s, count() AS cnt FROM v_05481 GROUP BY d, s ORDER BY cnt DESC LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)
    = (SELECT arraySort(groupArray(cnt)) FROM (SELECT d, s, count() AS cnt FROM v_05481 GROUP BY d, s ORDER BY cnt DESC LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, query_plan_aggregation_bucket_top_k = 0)
SETTINGS log_comment = '05481_keys64_bucket_top_k';

-- The shards and the initiator number the two-level buckets with the same method.
SELECT 'keys32, distributed two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM remote('127.0.0.{1,2}', currentDatabase(), v_05481) GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            distributed_aggregation_memory_efficient = 1, prefer_localhost_replica = 0)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM remote('127.0.0.{1,2}', currentDatabase(), v_05481) GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, prefer_localhost_replica = 0)
SETTINGS log_comment = '05481_keys32_distributed';

SELECT 'keys128, distributed two levels, serialized plan',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), min(c) FROM remote('127.0.0.{1,2}', currentDatabase(), v_05481) GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            distributed_aggregation_memory_efficient = 1, prefer_localhost_replica = 0, serialize_query_plan = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), min(c) FROM remote('127.0.0.{1,2}', currentDatabase(), v_05481) GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, prefer_localhost_replica = 0)
SETTINGS log_comment = '05481_keys128_distributed_plan';

SYSTEM FLUSH LOGS query_log;

SELECT 'two levels', log_comment,
    ProfileEvents['AggregationConvertedToTwoLevel'] + ProfileEvents['AggregationHashTablesInitializedAsTwoLevel'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('05481_keys32_two_level', '05481_keys128_two_level', '05481_keys32_external')
ORDER BY log_comment;

SELECT 'preallocated', log_comment, ProfileEvents['AggregationPreallocatedElementsInHashTables'] > 0,
    ProfileEvents['AggregationHashTablesInitializedAsTwoLevel'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('05481_keys32_preallocated', '05481_keys32_preallocated_two_level')
ORDER BY log_comment;

SELECT 'written to disk', log_comment, ProfileEvents['ExternalAggregationWritePart'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05481_keys32_external';

SELECT 'top-K heap', log_comment, ProfileEvents['AggregationTopKRowsSkipped'] + ProfileEvents['AggregationTopKKeysEvicted'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05481_keys32_top_k_heap';

SELECT 'bucket top-K', log_comment, ProfileEvents['AggregationBucketTopKConversions'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05481_keys64_bucket_top_k';

DROP VIEW v_05481;
DROP VIEW v_05481_preallocated;
DROP TABLE t_05481_sorted;
