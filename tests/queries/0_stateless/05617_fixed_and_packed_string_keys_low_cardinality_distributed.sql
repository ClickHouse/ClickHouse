-- Tags: distributed, no-parallel
-- Tag no-parallel: the shards of `test_cluster_two_shards_different_databases` read the databases `shard_0` and
-- `shard_1`, whose names are fixed.

-- A distributed `GROUP BY` returns each group once when one shard stores the string key as `String` and the other as
-- `LowCardinality(String)`, or when the `Distributed` table declares it as `LowCardinality(String)` over `String`
-- tables. The shards aggregate both column types with the same method and hash, so they number the two-level buckets
-- alike, and the initiator merges each bucket with the buckets of the same number. Without aggregate functions the
-- shards take the key-only forms of the methods, which hash the keys the same way.

DROP TABLE IF EXISTS dist_mixed_05617;
DROP TABLE IF EXISTS dist_mixed_low_cardinality_05617;
DROP TABLE IF EXISTS dist_string_as_low_cardinality_05617;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

-- 133,333 distinct pairs `(k, s)` on each shard, the same on both.
CREATE TABLE shard_0.t_mixed (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.t_mixed (k UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_0.t_string (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.t_string (k UInt64, s String) ENGINE = MergeTree ORDER BY k;

INSERT INTO shard_0.t_mixed SELECT number % 20000, if(number % 3 = 0, '', concat('string_', toString(number % 7))) FROM numbers(200000);
INSERT INTO shard_1.t_mixed SELECT * FROM shard_0.t_mixed;
INSERT INTO shard_0.t_string SELECT * FROM shard_0.t_mixed;
INSERT INTO shard_1.t_string SELECT * FROM shard_0.t_mixed;

CREATE TABLE dist_mixed_05617 (k UInt64, s String)
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_mixed);
CREATE TABLE dist_mixed_low_cardinality_05617 (k UInt64, s LowCardinality(String))
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_mixed);
CREATE TABLE dist_string_as_low_cardinality_05617 (k UInt64, s LowCardinality(String))
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_string);

-- The shards send two-level states, which the initiator merges bucket by bucket. Aggregation in order would send
-- single-level states, which the initiator merges in one piece.
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET distributed_aggregation_memory_efficient = 1;
SET optimize_aggregation_in_order = 0;
SET prefer_localhost_replica = 0;

SELECT 'groups', count() FROM (SELECT DISTINCT k, s FROM shard_0.t_mixed);

-- Each pair is a group with the rows of both shards.
SELECT 'String and LowCardinality(String) shards', count(), sum(c) FROM (SELECT k, s, count() AS c FROM dist_mixed_05617 GROUP BY k, s);
SELECT 'String and LowCardinality(String) shards, LowCardinality(String) Distributed',
    count(), sum(c) FROM (SELECT k, s, count() AS c FROM dist_mixed_low_cardinality_05617 GROUP BY k, s);
SELECT 'String shards, LowCardinality(String) Distributed',
    count(), sum(c) FROM (SELECT k, s, count() AS c FROM dist_string_as_low_cardinality_05617 GROUP BY k, s);

-- The key-only forms, merged bucket by bucket with the memory-efficient merge, and without it, when the initiator
-- takes the blocks of all buckets at once and groups them by bucket.
SELECT 'Key-only, String and LowCardinality(String) shards', count() FROM (SELECT k, s FROM dist_mixed_05617 GROUP BY k, s);
SELECT 'Key-only, String and LowCardinality(String) shards, no memory-efficient merge', count()
FROM (SELECT k, s FROM dist_mixed_05617 GROUP BY k, s)
SETTINGS distributed_aggregation_memory_efficient = 0;

-- The adaptive aggregator does not take `LowCardinality` keys, so the shard of `LowCardinality(String)` aggregates with
-- the ordinary aggregator, which numbers the buckets as the adaptive one does.
SELECT 'String and LowCardinality(String) shards, adaptive aggregator enabled', count(), sum(c)
FROM (SELECT k, s, count() AS c FROM dist_mixed_05617 GROUP BY k, s)
SETTINGS enable_adaptive_aggregator = 1;

SELECT 'String and LowCardinality(String) shards, adaptive aggregator disabled', count(), sum(c)
FROM (SELECT k, s, count() AS c FROM dist_mixed_05617 GROUP BY k, s)
SETTINGS enable_adaptive_aggregator = 0;

DROP TABLE dist_mixed_05617;
DROP TABLE dist_mixed_low_cardinality_05617;
DROP TABLE dist_string_as_low_cardinality_05617;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
