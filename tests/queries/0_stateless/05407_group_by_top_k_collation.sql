-- Tags: no-fasttest
-- no-fasttest: `COLLATE` needs ICU.
-- Random settings limits: max_bytes_before_external_group_by=(0, 0); max_bytes_ratio_before_external_group_by=(0, 0)

-- The `GROUP BY` top-K heap with a `COLLATE` on a matched key: it must rank groups in the collation order of the
-- final sort, on every path (single and composite keys, NULLs, threads, spilling, remote partial aggregation over
-- both the query text and a serialized plan), and a collated key gets no dynamic filter.
-- Every query runs with the optimization off and then on, so the reference holds identical pairs. The off run reads
-- in large blocks and does not spill, which does not change its result and keeps it cheap.
-- A collated heap looks a row's key up in the hash table before it compares the key with its boundary, so most cases
-- mix keys that repeat with keys that the heap skips. Many tables begin with distinct filler keys m0, m1, ..., which
-- under 'en' sort after a... and before Z..., so they are outside every tested result.

SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_trivial_group_by_limit_query = 0;
SET optimize_aggregation_in_order = 0;
SET exact_rows_before_limit = 0;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET group_by_top_k_optimization_observation_rows = 65536;
SET collect_hash_table_stats_during_aggregation = 0;
SET use_query_condition_cache = 0;
SET use_top_k_dynamic_filtering_for_variable_length_types = 0;

DROP TABLE IF EXISTS t_05407;
DROP TABLE IF EXISTS t_05407_spill;

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
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_composite_later';

-- ORDER BY shorter than GROUP BY: the heap ranks `s` only and skips rows without evicting groups.
-- LIMIT 4 takes both `b` of the two best `s`, so the selected set has no ties.
SELECT 'key prefix';
SELECT * FROM (SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 4) ORDER BY s, b
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT * FROM (SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 4) ORDER BY s, b
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1;
SELECT countIf(explain LIKE '%Top-K%collations=[en]%') FROM (EXPLAIN actions = 1
    SELECT s, b, count() FROM t_05407 GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 4);

-- Precomposed 'é' (C3A9) and 'e' + U+0301 (65CC81) are two groups that are equal under 'en'. After the 32800 fillers,
-- each cycle brings f5, f6, a, b, c, both forms of é and new f5<number> keys, one per row, so the heap trims with the
-- two forms tied at the boundary: both must survive. The trailing count decides which form the LIMIT keeps, so either
-- eviction shows.
SELECT 'ties under collation';
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('f5', toString(number)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c ASC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('f5', toString(number)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c ASC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 800, enable_group_by_top_k_optimization = 1, log_comment = '05407_ties_asc';
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('f5', toString(number)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c DESC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT hex(s), c FROM (SELECT s, count() AS c FROM (WITH (number - 32800) % 100 AS r SELECT if(number < 32800,
    concat('m', toString(number)), multiIf(r = 0, 'f5', r = 1, 'f6', r = 2, 'a', r = 3, 'b', r = 4, 'c',
    r = 5, char(0xC3, 0xA9), r = 6 AND intDiv(number - 32800, 100) % 3 != 2, concat('e', char(0xCC, 0x81)),
    concat('f5', toString(number)))) AS s FROM numbers(42800))
    GROUP BY s ORDER BY s ASC COLLATE 'en', c DESC LIMIT 4)
SETTINGS max_threads = 1, max_block_size = 800, enable_group_by_top_k_optimization = 1, log_comment = '05407_ties_desc';

-- 160000 unique fillers m0, m1, ... come first, then a tail of a/á/Z keys (each about 13 times), ranked across 4 streams.
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
-- The same with the adaptive aggregator.
SELECT s, count() FROM (SELECT if(number < 160000, concat('m', toString(number)),
    concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997))) AS s FROM numbers_mt(200000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 4, max_block_size = 65536, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT if(number < 160000, concat('m', toString(number)),
    concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997))) AS s FROM numbers_mt(200000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 4, max_block_size = 1024, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 1, log_comment = '05407_threads_adaptive';

-- After the fillers every row is a new key that ranks better than all before it, so the heap admits each one and the
-- arena grows with their 2000-byte payloads until the table crosses the two-level byte threshold and spills. The flush
-- recreates the table and its heap; the result comes from the rows after the payloads.
SELECT 'external aggregation';
CREATE TABLE t_05407_spill (s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024, index_granularity_bytes = 0;
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

-- A collated key gets no `__topKFilter`: it would compare every read row under the collation, also after the heap freezes.
SELECT 'dynamic filter';
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_group_by_top_k_dynamic_filtering = 1,
    max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_group_by_top_k_dynamic_filtering = 1,
    max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_dynamic_filter';
SELECT countIf(explain LIKE '%\_\_topKFilter%') FROM (EXPLAIN actions = 1
    SELECT s, count() FROM t_05407 GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
    SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1, enable_group_by_top_k_dynamic_filtering = 1);

-- A `COLLATE` on a key that cannot be collated is left to the sort, which rejects it.
SELECT 'not collatable';
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT b, count() FROM t_05407 GROUP BY b ORDER BY b ASC COLLATE 'en' LIMIT 1);
SELECT b, count() FROM t_05407 GROUP BY b ORDER BY b ASC COLLATE 'en' LIMIT 1; -- { serverError BAD_COLLATION }

SELECT 'repeated keys';
-- 2991 keys, each about 10 times.
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_small';
-- The same without `COLLATE`.
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat(['Z', 'a', 'á'][number % 3 + 1], toString(number % 997)) AS s FROM numbers(30000))
GROUP BY s ORDER BY s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_plain';
-- Every key comes twice, in consecutive rows.
SELECT s, count() FROM (SELECT concat('k', toString(intDiv(number, 2))) AS s FROM numbers(131072))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat('k', toString(intDiv(number, 2))) AS s FROM numbers(131072))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_repeats';
-- The repeated keys of the case above with a0..a3 among them, then unique u... keys, then rows of a1, á0, a4 that
-- find their keys in the table or rank into it.
SELECT s, count() FROM (SELECT multiIf(number < 131072 AND number % 1000 = 7, ['a0', 'a1', 'a2', 'a3'][intDiv(number, 1000) % 4 + 1],
    number < 131072, concat('k', toString(intDiv(number, 2))), number < 163840, concat('u', toString(number)),
    ['a1', 'á0', 'a4', 'zz'][number % 4 + 1]) AS s FROM numbers(165840))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT multiIf(number < 131072 AND number % 1000 = 7, ['a0', 'a1', 'a2', 'a3'][intDiv(number, 1000) % 4 + 1],
    number < 131072, concat('k', toString(intDiv(number, 2))), number < 163840, concat('u', toString(number)),
    ['a1', 'á0', 'a4', 'zz'][number % 4 + 1]) AS s FROM numbers(165840))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_repeats_then_unique';
-- The serialized method of a composite key compares each row with the boundary before the lookup.
SELECT lcs, s, count() FROM t_05407 GROUP BY lcs, s ORDER BY lcs ASC COLLATE 'en', s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT lcs, s, count() FROM t_05407 GROUP BY lcs, s ORDER BY lcs ASC COLLATE 'en', s ASC LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_serialized';
-- Every key twice in two streams with the adaptive aggregator.
SELECT s, count() FROM (SELECT concat('k', toString(intDiv(number, 2))) AS s FROM numbers_mt(160000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 65536, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT concat('k', toString(intDiv(number, 2))) AS s FROM numbers_mt(160000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 1024, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 1,
    log_comment = '05407_adaptive_repeats';

-- Other aggregate functions, and none. Each l... key ranks best and comes twice in a row, a0 and á0 repeat throughout,
-- so trims evict keys whose second row found them in the table earlier in the same block.
SELECT 'other aggregates';
SELECT s, count(), sum(number), uniqExact(number) FROM (WITH intDiv(number, 3) AS j SELECT number, multiIf(number % 3 = 0,
    if(j % 2 = 0, 'a0', 'á0'), concat('l', leftPad(toString(99999 - j), 5, '0'))) AS s FROM numbers(6144))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s, count(), sum(number), uniqExact(number) FROM (WITH intDiv(number, 3) AS j SELECT number, multiIf(number % 3 = 0,
    if(j % 2 = 0, 'a0', 'á0'), concat('l', leftPad(toString(99999 - j), 5, '0'))) AS s FROM numbers(6144))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_states';
SELECT s FROM (WITH intDiv(number, 3) AS j SELECT multiIf(number % 3 = 0,
    if(j % 2 = 0, 'a0', 'á0'), concat('l', leftPad(toString(99999 - j), 5, '0'))) AS s FROM numbers(6144))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, enable_group_by_top_k_optimization = 0;
SELECT s FROM (WITH intDiv(number, 3) AS j SELECT multiIf(number % 3 = 0,
    if(j % 2 = 0, 'a0', 'á0'), concat('l', leftPad(toString(99999 - j), 5, '0'))) AS s FROM numbers(6144))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, enable_group_by_top_k_optimization = 1, log_comment = '05407_keys';

-- After the fillers: 2000 n... keys in cycles, descending l... keys that each rank best, unique n... keys, or one key n;
-- a0..a4 every 97th row. `observation_rows = 1` judges the heap as early as it can.
SELECT 'judged heap';
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
-- After the unique keys only an existing key comes.
SELECT s, count() FROM (SELECT if(number < 32768, concat('m', toString(number)), 'm1') AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 65536, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (SELECT if(number < 32768, concat('m', toString(number)), 'm1') AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 1, max_block_size = 1024, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_never_fills';
-- The judged cases in two streams, one per side of the `UNION ALL`, with the adaptive aggregator.
SELECT s, count() FROM (
    SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('n', toString(number % 2000))) AS s FROM numbers(80000)
    UNION ALL
    SELECT multiIf(number < 33792, concat('p', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('n', toString(number % 2000))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 65536, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (
    SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('n', toString(number % 2000))) AS s FROM numbers(80000)
    UNION ALL
    SELECT multiIf(number < 33792, concat('p', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('n', toString(number % 2000))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 1024, group_by_two_level_threshold = 1000,
    group_by_two_level_threshold_bytes = 50000000, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_adaptive';
SELECT s, count() FROM (
    SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('n', toString(number))) AS s FROM numbers(80000)
    UNION ALL
    SELECT multiIf(number < 33792, concat('p', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('o', toString(number))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 65536, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (
    SELECT multiIf(number < 33792, concat('m', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('n', toString(number))) AS s FROM numbers(80000)
    UNION ALL
    SELECT multiIf(number < 33792, concat('p', toString(number)), number % 97 = 0, concat('a', toString(number % 5)),
        concat('o', toString(number))) AS s FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 1024, group_by_two_level_threshold = 1000,
    group_by_two_level_threshold_bytes = 50000000, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_adaptive_new_keys';
SELECT s, count() FROM (
    SELECT if(number < 33792, concat('m', toString(number)), concat('l', leftPad(toString(99999 - number), 5, '0'))) AS s
    FROM numbers(80000)
    UNION ALL
    SELECT if(number < 33792, concat('p', toString(number)), concat('k', leftPad(toString(99999 - number), 5, '0'))) AS s
    FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 65536, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 0;
SELECT s, count() FROM (
    SELECT if(number < 33792, concat('m', toString(number)), concat('l', leftPad(toString(99999 - number), 5, '0'))) AS s
    FROM numbers(80000)
    UNION ALL
    SELECT if(number < 33792, concat('p', toString(number)), concat('k', leftPad(toString(99999 - number), 5, '0'))) AS s
    FROM numbers(80000))
GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 2, max_block_size = 1024, group_by_two_level_threshold = 1000,
    group_by_two_level_threshold_bytes = 50000000, group_by_top_k_optimization_observation_rows = 1,
    enable_group_by_top_k_optimization = 1, log_comment = '05407_run_adaptive_new_best';

-- Every key comes twice and keys descend, so the prefix heap evicts on every new key; `observation_rows = 0` never
-- freezes it.
SELECT 'forced heap';
SELECT s, b, count() FROM (SELECT concat('l', leftPad(toString(99999 - intDiv(number, 2)), 5, '0')) AS s,
    intDiv(number, 2) % 2 AS b FROM numbers_mt(160000))
GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 4, max_block_size = 65536, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 0;
SELECT s, b, count() FROM (SELECT concat('l', leftPad(toString(99999 - intDiv(number, 2)), 5, '0')) AS s,
    intDiv(number, 2) % 2 AS b FROM numbers_mt(160000))
GROUP BY s, b ORDER BY s ASC COLLATE 'en' LIMIT 5
SETTINGS max_threads = 4, max_block_size = 1024, group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 50000000, enable_group_by_top_k_optimization = 1,
    group_by_top_k_optimization_observation_rows = 0, log_comment = '05407_forced_repeats';

SYSTEM FLUSH LOGS query_log;

-- Whether the heap pruned (on the shards for the remote queries), the aggregation spilled, and the heap trimmed in the
-- tie and other-aggregate cases.
SELECT 'engaged';
SELECT
    i.log_comment,
    sum(q.ProfileEvents['AggregationTopKRowsSkipped'] + q.ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS pruned,
    i.log_comment != '05407_spill' OR sum(q.ProfileEvents['ExternalAggregationWritePart']) > 0 AS spilled,
    NOT (startsWith(i.log_comment, '05407_ties') OR i.log_comment IN ('05407_keys', '05407_states'))
        OR sum(q.ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS trimmed
FROM system.query_log AS q
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment IN ('05407_adaptive_repeats', '05407_composite_later', '05407_dynamic_filter',
            '05407_forced_repeats', '05407_keys', '05407_plain', '05407_remote_plan', '05407_remote_plan_nullable',
            '05407_remote_text', '05407_repeats', '05407_repeats_then_unique', '05407_run_adaptive',
            '05407_run_adaptive_new_best', '05407_run_adaptive_new_keys', '05407_run_never_fills',
            '05407_run_new_best', '05407_run_new_keys', '05407_run_one_key', '05407_run_repeats', '05407_serialized',
            '05407_small', '05407_spill', '05407_states', '05407_threads', '05407_threads_adaptive', '05407_ties_asc',
            '05407_ties_desc')
) AS i ON q.initial_query_id = i.query_id
WHERE q.type = 'QueryFinish' AND q.event_date >= yesterday() AND (q.is_initial_query = NOT startsWith(i.log_comment, '05407_remote'))
GROUP BY i.log_comment
ORDER BY i.log_comment;

DROP TABLE t_05407;
DROP TABLE t_05407_spill;
