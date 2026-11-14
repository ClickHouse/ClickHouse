-- Tags: distributed

-- With parallel_distributed_insert_select the INSERT SELECT between two Distributed tables on the
-- same cluster is rewritten and sent to the shards, and the initiator never builds a local insert
-- pipeline. The target table and its columns must still be recorded in the initiator's row of
-- system.query_log.

DROP TABLE IF EXISTS dst_dist;
DROP TABLE IF EXISTS src_dist;
DROP TABLE IF EXISTS dst_local;
DROP TABLE IF EXISTS src_local;

CREATE TABLE src_local (srcKey UInt32, srcVal String) ENGINE = MergeTree ORDER BY srcKey;
CREATE TABLE dst_local (dstKey UInt32, dstVal String) ENGINE = MergeTree ORDER BY dstKey;
CREATE TABLE src_dist AS src_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), src_local);
CREATE TABLE dst_dist AS dst_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), dst_local);

INSERT INTO src_local VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- prefer_localhost_replica = 0 sends the rewritten query over the network even to the local shard.
INSERT INTO dst_dist
SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, log_comment = '05499_parallel_distributed_insert_select'
SELECT * FROM src_dist;

SELECT * FROM dst_local ORDER BY dstKey;

SYSTEM FLUSH LOGS query_log;

-- The initiator's row comes first. The second row is the query forwarded to the shard and proves
-- that the distributed path was taken.
SELECT is_initial_query, tables, columns
FROM system.query_log
WHERE type = 'QueryFinish'
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND is_initial_query
        AND log_comment = '05499_parallel_distributed_insert_select')
ORDER BY is_initial_query DESC
FORMAT Vertical;

DROP TABLE dst_dist;
DROP TABLE src_dist;
DROP TABLE dst_local;
DROP TABLE src_local;
