-- JOIN runtime filters represent many integer keys over a narrow range as a dense bitset instead of a Bloom filter.
-- The result never depends on the filter.

SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET enable_join_runtime_filters_index_analysis = 0;
SET join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_bloom_filter_bytes = 524288;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build;

CREATE TABLE probe (k Int64, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE build (k Int64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES build;

INSERT INTO probe SELECT number % 300000 - 150000, number FROM numbers(1000000);
-- Two parts, inserted high keys first, so the bitset of one stream grows downwards and the partial filters of several streams are merged.
INSERT INTO build SELECT number * 3 FROM numbers(15000);
INSERT INTO build SELECT number * 3 - 45000 FROM numbers(15000);

SELECT 'dense inner', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS log_comment = '05497 dense inner', max_threads = 4;
SELECT 'dense inner', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'dense inner one thread', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS log_comment = '05497 dense inner one thread', max_threads = 1;
-- 1 KiB of Bloom bits would be saturated by these keys and dropped; the bitset (16 x 1 KiB covers 131072 values) keeps checking rows.
SELECT 'dense stays', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS log_comment = '05497 dense stays', join_runtime_bloom_filter_bytes = 1024, join_runtime_filter_size_from_hash_table_stats = 0, max_threads = 1, max_block_size = 8192;
SELECT 'dense stays', count(), sum(v) FROM probe AS p INNER JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'dense right', count(), sum(v) FROM probe AS p RIGHT JOIN build AS b ON p.k = b.k;
SELECT 'dense right', count(), sum(v) FROM probe AS p RIGHT JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'dense semi', count(), sum(v) FROM probe AS p LEFT SEMI JOIN build AS b ON p.k = b.k;
SELECT 'dense semi', count(), sum(v) FROM probe AS p LEFT SEMI JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'dense anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k;
SELECT 'dense anti', count(), sum(v) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

-- Narrower types, including a probe value that only matches after the cast to the filter type.
SELECT 'Int32', count(), sum(v) FROM (SELECT toInt32(k) AS k, v FROM probe) AS p INNER JOIN (SELECT toInt32(k) AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = '05497 Int32';
SELECT 'Int32', count(), sum(v) FROM (SELECT toInt32(k) AS k, v FROM probe) AS p INNER JOIN (SELECT toInt32(k) AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'UInt32 and Int64', count(), sum(v) FROM probe AS p INNER JOIN (SELECT toUInt32(k + 45000) AS k FROM build) AS b ON p.k = b.k;
SELECT 'UInt32 and Int64', count(), sum(v) FROM probe AS p INNER JOIN (SELECT toUInt32(k + 45000) AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

-- A key range beyond the allowance keeps the Bloom filter: 16 x join_runtime_bloom_filter_bytes = 8 MiB covers 67M values.
SELECT 'sparse', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k * 10000 AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = '05497 sparse';
SELECT 'sparse', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k * 10000 AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
-- 60M values need 7.5 MB: within the default allowance, beyond the allowance of a smaller Bloom filter setting.
SELECT 'wide', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k * 666 AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = '05497 wide';
-- One stream: with many streams, the short run of keys that one stream sees can fit a bitset before the merge moves them to a Bloom filter.
SELECT 'wide small allowance', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k * 666 AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = '05497 wide small allowance', join_runtime_bloom_filter_bytes = 65536, max_threads = 1, max_block_size = 8192;
SELECT 'wide', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k * 666 AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

-- A key far above the range arrives last, after the bitset was built: the keys move to a Bloom filter and none is lost.
-- Blocks smaller than the build side, so the bitset is built before the far keys arrive.
SELECT 'fallback', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k FROM (SELECT k FROM build UNION ALL SELECT toInt64(1000000000000 + number) FROM numbers(3)) ORDER BY k) AS b ON p.k = b.k SETTINGS log_comment = '05497 fallback', max_threads = 1, max_block_size = 8192;
SELECT 'fallback', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k FROM (SELECT k FROM build UNION ALL SELECT toInt64(1000000000000 + number) FROM numbers(3)) ORDER BY k) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

-- UInt64 keys above the Int64 range keep the Bloom filter.
SELECT 'UInt64 high', count(), sum(v) FROM (SELECT toUInt64(k + 150000) + 18446744073709000000 AS k, v FROM probe) AS p INNER JOIN (SELECT toUInt64(k + 150000) + 18446744073709000000 AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = '05497 UInt64 high';
SELECT 'UInt64 high', count(), sum(v) FROM (SELECT toUInt64(k + 150000) + 18446744073709000000 AS k, v FROM probe) AS p INNER JOIN (SELECT toUInt64(k + 150000) + 18446744073709000000 AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

-- Few keys stay in the exact set; Nullable keys keep the exact set and the Bloom filter.
SELECT 'few keys', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k FROM build WHERE k % 12 = 0) AS b ON p.k = b.k SETTINGS log_comment = '05497 few keys';
SELECT 'few keys', count(), sum(v) FROM probe AS p INNER JOIN (SELECT k FROM build WHERE k % 12 = 0) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'Nullable', count(), sum(v) FROM (SELECT if(v % 7 = 0, NULL, k) AS k, v FROM probe) AS p INNER JOIN (SELECT if(k % 11 = 0, NULL, k) AS k FROM build) AS b ON p.k = b.k SETTINGS log_comment = '05497 Nullable';
SELECT 'Nullable', count(), sum(v) FROM (SELECT if(v % 7 = 0, NULL, k) AS k, v FROM probe) AS p INNER JOIN (SELECT if(k % 11 = 0, NULL, k) AS k FROM build) AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RuntimeFilterDenseBitsetsBuilt'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05497 %'
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['RuntimeFilterDenseBitsetsBuilt'] > 0, ProfileEvents['RuntimeFilterRowsChecked'] > 0, ProfileEvents['RuntimeFilterBloomFilterBuildsSkipped'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05497 dense stays';

DROP TABLE probe;
DROP TABLE build;
