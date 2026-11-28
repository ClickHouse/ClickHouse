-- Tags: distributed
-- Each shard folds `shardNum()` in the `IN` subquery, so its set name differs from the initiator's (#113183).

SELECT sum(number IN (SELECT number FROM numbers(10) WHERE number = shardNum())) FROM remote('127.0.0.{1,2}', numbers(10));

SELECT countIf(number IN (SELECT number FROM numbers(10) WHERE number = shardNum()))
FROM remote('127.0.0.{1,2}', numbers(10)) SETTINGS prefer_localhost_replica = 0;

SELECT number IN (SELECT number FROM numbers(10) WHERE number = shardNum()) AS k, count()
FROM remote('127.0.0.{1,2}', numbers(10)) GROUP BY k ORDER BY k;

SELECT
    countIf(number IN (SELECT number FROM numbers(10) WHERE number = shardNum())),
    countIf(number IN (SELECT number FROM numbers(10) WHERE number <= shardNum()))
FROM remote('127.0.0.{1,2}', numbers(10)) SETTINGS prefer_localhost_replica = 0;

-- Both sets are `number = 1` on shard 1, so it returns one column for them.
SELECT
    countIf(number IN (SELECT number FROM numbers(10) WHERE number = shardNum())),
    countIf(number IN (SELECT number FROM numbers(10) WHERE number = toUInt32(1)))
FROM remote('127.0.0.{1,2}', numbers(10)) SETTINGS prefer_localhost_replica = 0;

-- Single-level only: a two-level block is split into buckets by the shard's own keys.
SELECT
    number IN (SELECT number FROM numbers(10) WHERE number = shardNum()) AS a,
    number IN (SELECT number FROM numbers(10) WHERE number = toUInt32(1)) AS b,
    count()
FROM remote('127.0.0.{1,2}', numbers(10)) GROUP BY a, b ORDER BY a, b
SETTINGS prefer_localhost_replica = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;
CREATE TABLE t_local (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local SELECT number FROM numbers(10);
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_local);

SELECT countIf(x IN (SELECT number FROM numbers(10) WHERE number = shardNum())) FROM t_dist;

DROP TABLE t_dist;
DROP TABLE t_local;
