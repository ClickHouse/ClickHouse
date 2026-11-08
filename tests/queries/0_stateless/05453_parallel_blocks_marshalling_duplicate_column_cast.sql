-- Tags: shard

-- With parallel blocks marshalling, a remote query reading one column into several outputs
-- applied the cast of the received column once per output, and the repeated cast failed.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_dup_cast;
CREATE TABLE t_dup_cast (i LowCardinality(Int32), s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dup_cast VALUES (1, 'a'), (2, 'b'), (3, 'c');

SET enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0, low_cardinality_allow_in_native_format = 0;

SELECT * FROM (SELECT i, i FROM remote('127.0.0.{1,2}', currentDatabase(), t_dup_cast) LIMIT 100) ORDER BY ALL;
SELECT * FROM (SELECT i AS a, i AS b FROM remote('127.0.0.{1,2}', currentDatabase(), t_dup_cast) LIMIT 100) ORDER BY ALL;
SELECT * FROM (SELECT *, i FROM remote('127.0.0.{1,2}', currentDatabase(), t_dup_cast) LIMIT 100) ORDER BY ALL;
SELECT * FROM (SELECT s, s FROM remote('127.0.0.{1,2}', currentDatabase(), t_dup_cast) LIMIT 100) ORDER BY ALL;
SELECT * FROM (SELECT i, i FROM remote('127.0.0.{1,2}', currentDatabase(), t_dup_cast)) ORDER BY ALL SETTINGS distributed_group_by_no_merge = 2;

DROP TABLE t_dup_cast;
