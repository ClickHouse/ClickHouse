-- Tags: no-ordinary-database
-- `y` is an ordinary column of the `Distributed` table, but an `ALIAS` of the table behind it. The
-- initiator analyzes only the schema of the `Distributed` table, and the shard that expands the alias has
-- no consistency capture, so the probe of the table behind it must check the resolved alias: the overload
-- resolver of `arrayReduce('groupArraySample(1)', ...)` is deterministic, the function it builds is not.
-- An unseeded alias leaves the table, and the `Distributed` table over it, without a modification hash,
-- and the consistent query cache does not cache the result. The seeded alias is deterministic.

DROP TABLE IF EXISTS dist_unseeded_05459;
DROP TABLE IF EXISTS dist_seeded_05459;
DROP TABLE IF EXISTS unseeded_05459;
DROP TABLE IF EXISTS seeded_05459;

CREATE TABLE unseeded_05459 (k UInt64, y Array(UInt64) ALIAS arrayReduce('groupArraySample(1)', range(1000))) ENGINE = MergeTree ORDER BY k;
INSERT INTO unseeded_05459 VALUES (1);
CREATE TABLE seeded_05459 (k UInt64, y Array(UInt64) ALIAS arrayReduce('groupArraySample(1, 42)', range(1000))) ENGINE = MergeTree ORDER BY k;
INSERT INTO seeded_05459 VALUES (1);
CREATE TABLE dist_unseeded_05459 (k UInt64, y Array(UInt64)) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), unseeded_05459);
CREATE TABLE dist_seeded_05459 (k UInt64, y Array(UInt64)) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), seeded_05459);

SELECT name, modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name LIKE '%seeded_05459' ORDER BY name;

SET use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SET serialize_query_plan = 0, prefer_localhost_replica = 0;

SELECT 'unseeded', length(y) FROM dist_unseeded_05459;
SELECT 'unseeded', length(y) FROM dist_unseeded_05459;
SELECT 'seeded', length(y) FROM dist_seeded_05459;
SELECT 'seeded', length(y) FROM dist_seeded_05459;

SET use_query_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT extract(query, '^SELECT \'([a-z]+)\''), ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish' AND is_initial_query
  AND current_database = currentDatabase() AND query LIKE 'SELECT \'%seeded%'
ORDER BY event_time_microseconds;

DROP TABLE dist_unseeded_05459;
DROP TABLE dist_seeded_05459;
DROP TABLE unseeded_05459;
DROP TABLE seeded_05459;
