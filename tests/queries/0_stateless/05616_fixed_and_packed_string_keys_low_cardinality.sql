-- Tags: shard

-- A `GROUP BY` over fixed-width keys and one `LowCardinality(String)` key gives the same result with the methods of
-- such keys (`keys32_packed_string`, `keys64_packed_string`, `keys128_packed_string`) as with the serialized method,
-- on every path of the ordinary aggregator. The methods read the strings of the key from its dictionary: a dictionary of
-- each block for a column that the query computes, and the dictionaries that the blocks of a part share for a column
-- read from a `MergeTree` table. A `LowCardinality` fixed-width key is materialized: as the blocks are aggregated, and on
-- the initiator as it merges the states of the shards. Each comparison runs the path under test with the method on in
-- one subquery and the plain path with the method off in the other, so the profile events of the query show that the
-- path under test ran.

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
-- The profile of the test server limits the number of groups, and an aggregation with such a limit keeps no top-K
-- heap. The distributed-plan suite serializes every plan, which the top-K plan passes refuse.
SET max_rows_to_group_by = 0;
SET serialize_query_plan = 0;
SET log_queries = 1;
SET log_profile_events = 1;

DROP VIEW IF EXISTS v_05616;
DROP TABLE IF EXISTS t_05616;
DROP TABLE IF EXISTS t_05616_fixed;

-- `s` has empty strings, strings of zero bytes, and strings kept inline or stored out of line, with too many values for
-- one dictionary per part. `t` has 501 values, so each part keeps one dictionary for it.
CREATE VIEW v_05616 AS
SELECT
    toUInt32(intDiv(number, 3) % 4000) AS a,
    toUInt8(number % 3) AS b,
    toUInt16(number % 5) AS c,
    intDiv(number, 3) % 4000 * 1000003 AS d,
    toLowCardinality(multiIf(
        number % 29 = 0, '',
        number % 29 = 1, repeat('\0', 1 + number % 3),
        number % 29 = 2, repeat('y', 40 + number % 50),
        substring(concat(hex(cityHash64(intDiv(number, 7))), hex(cityHash64(intDiv(number, 11)))), 1, number % 29 - 2))) AS s,
    toLowCardinality(if(number % 7 = 0, '', concat('value_', toString(number % 500)))) AS t
FROM numbers_mt(120000);

-- Three parts, one for each value of `b`.
CREATE TABLE t_05616 (c UInt16, a UInt32, b UInt8, d UInt64, s LowCardinality(String), t LowCardinality(String))
ENGINE = MergeTree ORDER BY (c, a) SETTINGS index_granularity = 1024;
INSERT INTO t_05616 SELECT c, a, b, d, s, t FROM v_05616 WHERE b = 0;
INSERT INTO t_05616 SELECT c, a, b, d, s, t FROM v_05616 WHERE b = 1;
INSERT INTO t_05616 SELECT c, a, b, d, s, t FROM v_05616 WHERE b = 2;

-- A `LowCardinality` fixed-width key in the sorting key.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_05616_fixed (c UInt16, a LowCardinality(UInt32), s String, d UInt64)
ENGINE = MergeTree ORDER BY (c, a) SETTINGS index_granularity = 1024;
INSERT INTO t_05616_fixed SELECT c, a, s, d FROM v_05616;

-- `keys32_packed_string` has a key layout of its own, so the dictionaries of each block are read with both layouts.
SELECT 'keys32, dictionary of each block',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM v_05616 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM v_05616 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys32_block_dictionary';

SELECT 'keys64, dictionary of each block',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, count(), max(a) FROM v_05616 GROUP BY d, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, count(), max(a) FROM v_05616 GROUP BY d, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys64_block_dictionary';

-- The blocks of a part share its dictionary of `t`, and each block hashes the entries that its rows use.
SELECT 'keys32, one dictionary per part',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM t_05616 GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM t_05616 GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys32_part_dictionary';

SELECT 'keys128, several dictionaries per part',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), min(c) FROM t_05616 GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), min(c) FROM t_05616 GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys128_part_dictionaries';

-- The string key may be a `String` next to a `LowCardinality` fixed-width key.
SELECT 'keys32, LowCardinality fixed-width key',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT toLowCardinality(a) AS la, CAST(s, 'String') AS ps, count() FROM v_05616 GROUP BY la, ps)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT toLowCardinality(a) AS la, CAST(s, 'String') AS ps, count() FROM v_05616 GROUP BY la, ps)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys32_low_cardinality_fixed';

SELECT 'keys32, two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM t_05616 GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM t_05616 GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys32_two_level';

SELECT 'keys64, external aggregation',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, count(), uniqExact(b) FROM v_05616 GROUP BY d, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            max_bytes_before_external_group_by = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, count(), uniqExact(b) FROM v_05616 GROUP BY d, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys64_external';

SELECT 'keys32, partitioned single-level merge',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), uniqExact(d) FROM t_05616 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_parallel_single_level_merge = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), uniqExact(d) FROM t_05616 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys32_partitioned_merge';

-- Aggregation in order hashes every run of equal values of the sorting key prefix with a state of its own, which
-- hashes the strings of its rows and packs a materialized copy of its rows of a `LowCardinality` fixed-width key.
SELECT 'keys64, aggregation in order',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, a, s, count(), sum(d) FROM t_05616 GROUP BY c, a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, optimize_aggregation_in_order = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, a, s, count(), sum(d) FROM t_05616 GROUP BY c, a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys64_in_order';

SELECT 'keys64, aggregation in order, LowCardinality fixed-width key',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, a, s, count(), sum(d) FROM t_05616_fixed GROUP BY c, a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, optimize_aggregation_in_order = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT c, a, s, count(), sum(d) FROM t_05616_fixed GROUP BY c, a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys64_in_order_low_cardinality_fixed';

-- The bounded heap of `ORDER BY` keys `LIMIT` reads the keys of the rows that it ranks from full columns.
SELECT 'keys32, ORDER BY keys LIMIT',
    (SELECT groupArray(tuple(*)) FROM (SELECT a, t, count() FROM t_05616 GROUP BY a, t ORDER BY a DESC, t LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT a, t, count() FROM t_05616 GROUP BY a, t ORDER BY a DESC, t LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_group_by_top_k_optimization = 0)
SETTINGS log_comment = '05616_keys32_top_k_heap';

-- The initiator merges the two-level states of the shards with the chosen method, which reads their `LowCardinality`
-- key columns in place. The memory-efficient merge would use `prealloc_serialized_hash64` instead.
SELECT 'keys32, distributed two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM remote('127.0.0.{1,2}', currentDatabase(), t_05616) GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            distributed_aggregation_memory_efficient = 0, prefer_localhost_replica = 0)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM remote('127.0.0.{1,2}', currentDatabase(), t_05616) GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, prefer_localhost_replica = 0)
SETTINGS log_comment = '05616_keys32_distributed';

SELECT 'keys32, LowCardinality fixed-width key, distributed two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT toLowCardinality(a) AS la, s, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_05616) GROUP BY la, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
            distributed_aggregation_memory_efficient = 0, prefer_localhost_replica = 0)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT toLowCardinality(a) AS la, s, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_05616) GROUP BY la, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, prefer_localhost_replica = 0)
SETTINGS log_comment = '05616_keys32_low_cardinality_fixed_distributed';

-- The adaptive aggregator does not take `LowCardinality` keys, so the ordinary one aggregates them.
SELECT 'keys32, adaptive aggregator enabled',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM t_05616 GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, t, count(), sum(d) FROM t_05616 GROUP BY a, t)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0)
SETTINGS log_comment = '05616_keys32_adaptive';

SYSTEM FLUSH LOGS query_log;

SELECT 'two levels', log_comment,
    ProfileEvents['AggregationConvertedToTwoLevel'] + ProfileEvents['AggregationHashTablesInitializedAsTwoLevel'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('05616_keys32_two_level', '05616_keys64_external')
ORDER BY log_comment;

SELECT 'written to disk', log_comment, ProfileEvents['ExternalAggregationWritePart'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05616_keys64_external';

SELECT 'top-K heap', log_comment, ProfileEvents['AggregationTopKRowsSkipped'] + ProfileEvents['AggregationTopKKeysEvicted'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05616_keys32_top_k_heap';

DROP VIEW v_05616;
DROP TABLE t_05616;
DROP TABLE t_05616_fixed;
