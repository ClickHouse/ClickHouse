-- Tags: no-fasttest
-- no-fasttest: `COLLATE` needs ICU.
-- Random settings limits: max_bytes_before_external_group_by=(0, 0); max_bytes_ratio_before_external_group_by=(0, 0)

-- The `GROUP BY` top-K heap with a `COLLATE` on a matched key: it must rank groups in the collation order of the
-- final sort, on every path (single and composite keys, NULLs, threads, spilling, remote partial aggregation over
-- both the query text and a serialized plan, the dynamic filter and its query condition cache entries).
-- Every query runs with the optimization off and then on, so the reference holds identical pairs. The off run reads
-- in large blocks and does not spill, which does not change its result and keeps it cheap.
-- A collated heap starts only once a stream's table holds 32768 keys and its last block inserted almost only new
-- keys, so the tables begin with 33792 distinct starter keys m0, m1, ..., which a single stream reads first. A read
-- can return a whole granule as one block, so `index_granularity` stays at most 1024 to start the heap at their end.
-- Under 'en' they sort after a... and before Z..., so they are outside every tested result.

SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_trivial_group_by_limit_query = 0;
SET optimize_aggregation_in_order = 0;
SET exact_rows_before_limit = 0;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET group_by_top_k_optimization_observation_rows = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET use_query_condition_cache = 0;
SET use_top_k_dynamic_filtering_for_variable_length_types = 0;

DROP TABLE IF EXISTS t_05407;
DROP TABLE IF EXISTS t_05407_spill;
DROP TABLE IF EXISTS t_05407_qcc;

-- Under 'en': a0 < á0 < a1 < á1 < a10 < ... < m... < Z...; in bytes: Z... < a... < m... < á...
CREATE TABLE t_05407 (s String, ns Nullable(String), lcs LowCardinality(String), b UInt8)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
INSERT INTO t_05407 SELECT s, if(number >= 33792 AND (number - 33792) % 101 = 0, NULL, s), s, number % 2
FROM (SELECT number, if(number < 33792, concat('m', toString(number)),
    concat(['Z', 'a', 'á'][(number - 33792) % 3 + 1], toString((number - 33792) % 997))) AS s FROM numbers(63792))
SETTINGS max_insert_threads = 1;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_05407' AND active;

SELECT 'single key';
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s DESC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s DESC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;
SELECT countIf(explain LIKE '%Top-K%collations=[en]%') FROM (EXPLAIN actions = 1
    SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5);

SELECT 'nullable key';
SELECT ns, count() FROM t_05407 GROUP BY ns ORDER BY ns ASC NULLS FIRST COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT ns, count() FROM t_05407 GROUP BY ns ORDER BY ns ASC NULLS FIRST COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;
SELECT ns, count() FROM t_05407 GROUP BY ns ORDER BY ns DESC NULLS LAST COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT ns, count() FROM t_05407 GROUP BY ns ORDER BY ns DESC NULLS LAST COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;

SELECT 'low cardinality key';
SELECT lcs, count() FROM t_05407 GROUP BY lcs ORDER BY lcs ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT lcs, count() FROM t_05407 GROUP BY lcs ORDER BY lcs ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;

SELECT 'composite keys';
SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s DESC COLLATE 'en', b ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s DESC COLLATE 'en', b ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;
SELECT b, s, count() FROM t_05407 GROUP BY b, s ORDER BY b ASC, s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT b, s, count() FROM t_05407 GROUP BY b, s ORDER BY b ASC, s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;

-- ORDER BY shorter than GROUP BY: the heap ranks `s` only and skips rows without evicting groups.
-- LIMIT 4 takes both `b` of the two best `s`, so the selected set has no ties.
SELECT 'key prefix';
SELECT * FROM (SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 4) ORDER BY s, b
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT * FROM (SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 4) ORDER BY s, b
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;
SELECT countIf(explain LIKE '%Top-K%collations=[en]%') FROM (EXPLAIN actions = 1
    SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 4);

-- Precomposed 'é' (C3A9) and 'e' + U+0301 (65CC81) are two groups that are equal under 'en'. The heap starts right
-- after the 32800 starters; its first cycle fills it with f5, f6, a, b and then admits c and both forms of é, so the
-- first trim has the two forms tied at the boundary: both must survive. The trailing count decides which form the
-- LIMIT keeps, so either eviction shows.
SELECT 'ties under collation';
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('g', toString(r)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c ASC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('g', toString(r)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c ASC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 800, enable_group_by_top_k_optimization = 1, log_comment = '05407_ties_asc';
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('g', toString(r)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c DESC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('g', toString(r)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c DESC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 800, enable_group_by_top_k_optimization = 1, log_comment = '05407_ties_desc';

-- 160000 unique starters m0, m1, ... come first, so at least one of the 4 streams reads 32768 of them and starts its
-- heap; the tail of a/á/Z keys (each about 13 times) is then ranked across streams that did and did not start.
SELECT 'threads';
SELECT s, count() FROM (SELECT if(number < 160000, concat('m', toString(number)),
    concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997))) AS s FROM numbers_mt(200000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 4, max_block_size = 65536, enable_adaptive_aggregator = 0, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT if(number < 160000, concat('m', toString(number)),
    concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997))) AS s FROM numbers_mt(200000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 4, max_block_size = 1024, enable_adaptive_aggregator = 0, enable_group_by_top_k_optimization = 1,
    log_comment = '05407_threads';

-- After the starters every row is a new key that ranks better than all before it, so the started heap admits each
-- one and the arena grows with their 2000-byte payloads until the table crosses the two-level byte threshold and
-- spills. The flush recreates the table, whose heap waits again; the result comes from the rows after the payloads.
SELECT 'external aggregation';
CREATE TABLE t_05407_spill (s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
INSERT INTO t_05407_spill SELECT multiIf(number < 33792, concat('m', toString(number)),
    number < 37888, concat('n', leftPad(toString(number), 6, '0'), repeat('x', 2000)),
    concat(['Z', 'a', 'á'][(number - 37888) % 3 + 1], toString((number - 37888) % 997)))
FROM numbers(67888) SETTINGS max_insert_threads = 1;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_05407_spill' AND active;
SELECT s, count() FROM t_05407_spill GROUP BY s ORDER BY s DESC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM t_05407_spill GROUP BY s ORDER BY s DESC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 8000000,
    max_bytes_ratio_before_external_group_by = 0, max_bytes_before_external_group_by = 1, enable_group_by_top_k_optimization = 1,
    log_comment = '05407_spill';

-- Both shards run as secondary queries with one stream each; the counts double.
SELECT 'remote, query text';
SELECT s, count() FROM remote('127.0.0.1,localhost', currentDatabase(), t_05407) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0, max_threads = 1, max_block_size = 65536,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM remote('127.0.0.1,localhost', currentDatabase(), t_05407) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0, max_threads = 1, max_block_size = 1024,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_remote_text';

SELECT 'remote, serialized plan';
SELECT s, count() FROM remote('127.0.0.1,localhost', currentDatabase(), t_05407) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1, max_threads = 1, max_block_size = 65536,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM remote('127.0.0.1,localhost', currentDatabase(), t_05407) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1, max_threads = 1, max_block_size = 1024,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_remote_plan';
SELECT ns, count() FROM remote('127.0.0.1,localhost', currentDatabase(), t_05407) GROUP BY ns ORDER BY ns DESC NULLS FIRST COLLATE 'en' LIMIT 5
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1, max_threads = 1, max_block_size = 65536,
    enable_group_by_top_k_optimization = 0;
SELECT ns, count() FROM remote('127.0.0.1,localhost', currentDatabase(), t_05407) GROUP BY ns ORDER BY ns DESC NULLS FIRST COLLATE 'en' LIMIT 5
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1, max_threads = 1, max_block_size = 1024,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_remote_plan_nullable';

-- The heap publishes a collated boundary, so the `__topKFilter` of the read must compare under the same collation.
SELECT 'dynamic filter';
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_group_by_top_k_dynamic_filtering = 1,
    max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_group_by_top_k_dynamic_filtering = 1,
    max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_dynamic_filter';
SELECT countIf(explain LIKE '%\_\_topKFilter%') > 0 FROM (EXPLAIN actions = 1
    SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
    SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_group_by_top_k_dynamic_filtering = 1);

-- Under 'en' the boundary 'a' empties the granules of 'B'; in byte order 'B' is the answer, so the second query must
-- not reuse the query condition cache entries of the first.
SELECT 'query condition cache';
CREATE TABLE t_05407_qcc (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_05407_qcc SELECT number, multiIf(number < 33792, concat('m', toString(number)), number < 33920, 'a', 'B')
FROM numbers(34816) SETTINGS max_insert_threads = 1;
SELECT s, count() FROM t_05407_qcc WHERE id != 100000 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 1
SETTINGS use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, enable_group_by_top_k_dynamic_filtering = 1,
    use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1, max_block_size = 512,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_qcc';
SELECT s, count() FROM t_05407_qcc WHERE id != 100000 GROUP BY s ORDER BY s ASC LIMIT 1
SETTINGS use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, enable_group_by_top_k_dynamic_filtering = 1,
    use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1, max_block_size = 512,
    enable_group_by_top_k_optimization = 1;

-- A `COLLATE` on a key that cannot be collated is left to the sort, which rejects it.
SELECT 'not collatable';
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT b, count() FROM t_05407 GROUP BY b ORDER BY b ASC COLLATE 'en' LIMIT 1);
SELECT b, count() FROM t_05407 GROUP BY b ORDER BY b ASC COLLATE 'en' LIMIT 1; -- { serverError BAD_COLLATION }

SELECT 'start condition';
-- 2991 keys: the table never reaches the start size.
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_wait_small';
-- Without `COLLATE` the heap starts at once.
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_plain_at_once';
-- Every key comes twice: each block inserts new keys for half of its rows, however large the table gets.
SELECT s, count() FROM (SELECT concat('k', toString(intDiv(number, 2))) AS s FROM numbers(131072))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat('k', toString(intDiv(number, 2))) AS s FROM numbers(131072))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_wait_repeats';
-- The repeated keys of the case above with a0..a3 among them, then unique u... keys that start the heap on a later
-- check, then rows of a1, á0, a4: the result mixes keys aggregated before the start with keys inserted after it.
SELECT s, count() FROM (SELECT multiIf(number < 131072 AND number % 1000 = 7, ['a0', 'a1', 'a2', 'a3'][intDiv(number, 1000) % 4 + 1],
    number < 131072, concat('k', toString(intDiv(number, 2))), number < 163840, concat('u', toString(number)),
    ['a1', 'á0', 'a4', 'zz'][number % 4 + 1]) AS s FROM numbers(165840))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT multiIf(number < 131072 AND number % 1000 = 7, ['a0', 'a1', 'a2', 'a3'][intDiv(number, 1000) % 4 + 1],
    number < 131072, concat('k', toString(intDiv(number, 2))), number < 163840, concat('u', toString(number)),
    ['a1', 'á0', 'a4', 'zz'][number % 4 + 1]) AS s FROM numbers(165840))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_late_start';
-- The key preparation of the serialized method changes for the `LowCardinality` key when the heap starts.
SELECT lcs, s, count() FROM t_05407 GROUP BY lcs, s ORDER BY lcs ASC COLLATE 'en', s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT lcs, s, count() FROM t_05407 GROUP BY lcs, s ORDER BY lcs ASC COLLATE 'en', s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_late_lc';
-- A heap that has not started by the adaptive freeze gives up, and the table freezes.
SELECT s, count() FROM (SELECT concat('k', toString(number % 5000)) AS s FROM numbers_mt(200000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 65536, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat('k', toString(number % 5000)) AS s FROM numbers_mt(200000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 1024, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 1,
    log_comment = '05407_gave_up';

-- A started heap gives up when the keys it skips repeat (2000 n... keys in cycles, or one key n) or when it skips
-- nothing because every row is a new best key (descending l...), and keeps running while they stay new; a0..a4 every
-- 97th row.
SELECT 'running heap';
SELECT s, count() FROM (SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0,
    concat('a', toString(number % 5)), concat('n', toString(number % 2000))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0,
    concat('a', toString(number % 5)), concat('n', toString(number % 2000))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_repeats';
SELECT s, count() FROM (SELECT if(number < 33792, concat('m', toString(number)),
    concat('l', leftPad(toString(99999 - number), 5, '0'))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT if(number < 33792, concat('m', toString(number)),
    concat('l', leftPad(toString(99999 - number), 5, '0'))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_new_best';
SELECT s, count() FROM (SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0,
    concat('a', toString(number % 5)), concat('n', toString(number))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0,
    concat('a', toString(number % 5)), concat('n', toString(number))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_new_keys';
SELECT s, count() FROM (SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0,
    concat('a', toString(number % 5)), 'n') AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0,
    concat('a', toString(number % 5)), 'n') AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_one_key';
-- After the last block of unique keys only an existing key comes, so the heap never fills, and freezes.
SELECT s, count() FROM (SELECT if(number < 32768, concat('m', toString(number)), 'm1') AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT if(number < 32768, concat('m', toString(number)), 'm1') AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_never_fills';
-- A tail of one key, with unique keys in the first eighth of every block, still makes the heap give up after it has
-- skipped more keys than it samples (about 2M).
SELECT s, count() FROM (SELECT multiIf(number < 2200000, concat('m', toString(number)), number % 16384 < 2048,
    concat('u', toString(number)), 'n') AS s FROM numbers(3500000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 16384, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_after_sample';

SYSTEM FLUSH LOGS query_log;

-- Whether the heap pruned (on the shards for the remote queries), the aggregation spilled, and the table froze.
SELECT 'engaged';
SELECT
    i.log_comment,
    sum(q.ProfileEvents['AggregationTopKRowsSkipped'] + q.ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS pruned,
    i.log_comment != '05407_spill' OR sum(q.ProfileEvents['ExternalAggregationWritePart']) > 0 AS spilled,
    i.log_comment != '05407_gave_up' OR sum(q.ProfileEvents['AdaptiveAggregationLocalFreezes']) > 0 AS froze
FROM system.query_log AS q
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment IN ('05407_dynamic_filter', '05407_gave_up', '05407_late_lc', '05407_late_start', '05407_plain_at_once',
            '05407_qcc', '05407_remote_plan', '05407_remote_plan_nullable', '05407_remote_text', '05407_spill', '05407_threads',
            '05407_ties_asc', '05407_ties_desc', '05407_wait_repeats', '05407_wait_small')
) AS i ON q.initial_query_id = i.query_id
WHERE q.type = 'QueryFinish' AND q.event_date >= yesterday() AND (q.is_initial_query = NOT startsWith(i.log_comment, '05407_remote'))
GROUP BY i.log_comment
ORDER BY i.log_comment;

SELECT 'running heap engaged';
SELECT log_comment, sum(ProfileEvents['AggregationTopKRowsSkipped'] + ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS pruned,
    sum(ProfileEvents['AggregationTopKHeapsFrozen']) AS heaps_frozen
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05407_run_after_sample', '05407_run_never_fills', '05407_run_new_best', '05407_run_new_keys',
        '05407_run_one_key', '05407_run_repeats')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_05407;
DROP TABLE t_05407_spill;
DROP TABLE t_05407_qcc;
