-- Tags: atomic-database, shard
-- An incremental refresh from a `Distributed` table gets the cursor of every shard at the end of its stream. A query that
-- does not need the rest of the rows (a `DISTINCT` by a constant) would cancel the queries of the shards before that, so
-- the streams of the shards are read to the end, and a refresh without new rows in the source appends nothing.

DROP TABLE IF EXISTS des_mv_remote;
DROP TABLE IF EXISTS des_mv_local;
DROP TABLE IF EXISTS des_tgt_remote;
DROP TABLE IF EXISTS des_tgt_local;
DROP TABLE IF EXISTS des_d;
DROP TABLE IF EXISTS des_src;

CREATE TABLE des_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE des_d (k UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), des_src);
CREATE TABLE des_tgt_remote (c UInt8) ENGINE = MergeTree ORDER BY c;
CREATE TABLE des_tgt_local (c UInt8) ENGINE = MergeTree ORDER BY c;

INSERT INTO des_src SELECT number FROM numbers(100000);

CREATE MATERIALIZED VIEW des_mv_remote REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO des_tgt_remote EMPTY
    AS SELECT DISTINCT 1 AS c FROM des_d SETTINGS prefer_localhost_replica = 0, max_block_size = 1, max_threads = 1;
CREATE MATERIALIZED VIEW des_mv_local REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO des_tgt_local EMPTY
    AS SELECT DISTINCT 1 AS c FROM des_d SETTINGS prefer_localhost_replica = 1, max_block_size = 1, max_threads = 1;

SYSTEM REFRESH VIEW des_mv_remote;
SYSTEM WAIT VIEW des_mv_remote;
SYSTEM REFRESH VIEW des_mv_local;
SYSTEM WAIT VIEW des_mv_local;
SELECT 'first', (SELECT count() FROM des_tgt_remote), (SELECT count() FROM des_tgt_local);

-- No new rows in the source: nothing is appended.
SYSTEM REFRESH VIEW des_mv_remote;
SYSTEM WAIT VIEW des_mv_remote;
SYSTEM REFRESH VIEW des_mv_local;
SYSTEM WAIT VIEW des_mv_local;
SYSTEM REFRESH VIEW des_mv_remote;
SYSTEM WAIT VIEW des_mv_remote;
SYSTEM REFRESH VIEW des_mv_local;
SYSTEM WAIT VIEW des_mv_local;
SELECT 'idle', (SELECT count() FROM des_tgt_remote), (SELECT count() FROM des_tgt_local);

-- New rows: one more row.
INSERT INTO des_src VALUES (1000000);
SYSTEM REFRESH VIEW des_mv_remote;
SYSTEM WAIT VIEW des_mv_remote;
SYSTEM REFRESH VIEW des_mv_local;
SYSTEM WAIT VIEW des_mv_local;
SELECT 'new', (SELECT count() FROM des_tgt_remote), (SELECT count() FROM des_tgt_local);
SYSTEM REFRESH VIEW des_mv_remote;
SYSTEM WAIT VIEW des_mv_remote;
SYSTEM REFRESH VIEW des_mv_local;
SYSTEM WAIT VIEW des_mv_local;
SELECT 'idle', (SELECT count() FROM des_tgt_remote), (SELECT count() FROM des_tgt_local);

DROP TABLE des_mv_remote;
DROP TABLE des_mv_local;
DROP TABLE des_tgt_remote;
DROP TABLE des_tgt_local;
DROP TABLE des_d;
DROP TABLE des_src;
