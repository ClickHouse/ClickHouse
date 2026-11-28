-- `SHUFFLE` over nested `Distributed` tables: the intermediate servers receive the streams that are already shuffled
-- by the shards below, with the internal key column, and must merge them instead of adding another key.

SET allow_experimental_shuffle_query = 1;

DROP TABLE IF EXISTS shuffle_outer_dist;
DROP TABLE IF EXISTS shuffle_inner_dist;
DROP TABLE IF EXISTS shuffle_leaf;

CREATE TABLE shuffle_leaf (number UInt64) ENGINE = Memory;
INSERT INTO shuffle_leaf SELECT number FROM numbers(1000);
CREATE TABLE shuffle_inner_dist AS shuffle_leaf ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), shuffle_leaf);
CREATE TABLE shuffle_outer_dist AS shuffle_leaf ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), shuffle_inner_dist);

SELECT count(), uniqExact(number) FROM (SELECT number FROM shuffle_outer_dist SHUFFLE) SETTINGS prefer_localhost_replica = 0;
SELECT count(), uniqExact(number) FROM (SELECT number FROM shuffle_outer_dist SHUFFLE) SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM (SELECT number FROM shuffle_outer_dist SHUFFLE LIMIT 10) SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM (SELECT DISTINCT number FROM shuffle_outer_dist SHUFFLE) SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM (SELECT DISTINCT number FROM shuffle_outer_dist SHUFFLE LIMIT 10) SETTINGS prefer_localhost_replica = 0;
SELECT count(), uniqExact(k) FROM (SELECT number % 100 AS k FROM shuffle_outer_dist SHUFFLE LIMIT 1 BY number % 100) SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM (SELECT number FROM shuffle_outer_dist SHUFFLE LIMIT 1 BY number % 100 LIMIT 10) SETTINGS prefer_localhost_replica = 0;

DROP TABLE shuffle_outer_dist;
DROP TABLE shuffle_inner_dist;
DROP TABLE shuffle_leaf;
