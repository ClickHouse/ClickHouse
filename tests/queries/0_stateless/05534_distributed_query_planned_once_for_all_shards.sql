-- Tags: shard
-- The initiator plans the query of the shards once, whatever the number of shards.

SET prefer_localhost_replica = 0, serialize_query_plan = 0, optimize_skip_unused_shards = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT sum(number) FROM remote('127.0.0.{2,3}', numbers(10)) WHERE number % 2 FORMAT Null SETTINGS log_comment = '05534 2 shards';
SELECT sum(number) FROM remote('127.0.0.{2..5}', numbers(10)) WHERE number % 2 FORMAT Null SETTINGS log_comment = '05534 4 shards';

SYSTEM FLUSH LOGS query_log;

SELECT count(), sum(ProfileEvents['Shards']), uniqExact(ProfileEvents['QueriesWithSubqueries'])
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05534 % shards';

DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS dist;
CREATE TABLE data (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE dist AS data ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data, key);
INSERT INTO data SELECT number, number FROM numbers(4);

-- The query is rewritten for each shard.
SELECT 'rewritten IN';
SELECT key, _shard_num FROM dist WHERE key IN (0, 1, 2) ORDER BY ALL SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1;
SELECT key, _shard_num FROM dist WHERE key IN (0, 1, 2) ORDER BY ALL SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 0;

SELECT 'local and remote shards';
SELECT key, count(), uniqExact(_shard_num) FROM dist GROUP BY key ORDER BY key SETTINGS prefer_localhost_replica = 1;
SELECT key, count(), uniqExact(_shard_num) FROM dist GROUP BY key ORDER BY key SETTINGS prefer_localhost_replica = 0;

SELECT 'filter';
SELECT * FROM (SELECT key, sum(value) AS s FROM dist GROUP BY key) WHERE key > 1 ORDER BY key;

-- One filter for each of the shards, and one for the initiator.
SELECT countIf(explain LIKE '%Filter column: greater(%') FROM
(
    EXPLAIN actions = 1, distributed = 1
    SELECT * FROM (SELECT number AS key FROM remote('127.0.0.{2..5}', numbers(10))) WHERE key > 1
);

SELECT 'grouping';
SELECT key, grouping(key) AS g, count() FROM dist GROUP BY GROUPING SETS ((key), ()) ORDER BY g, key;

DROP TABLE dist;
DROP TABLE data;
