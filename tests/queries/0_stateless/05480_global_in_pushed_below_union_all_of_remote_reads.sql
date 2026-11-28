-- Tags: shard, no-replicated-database

-- A GLOBAL IN filter pushed below UNION ALL into the remote reads of each branch must send every
-- branch's shards the filled set: https://github.com/ClickHouse/ClickHouse/issues/128121

-- Keep the reads on the path that splices the pushed-down filter into the query text sent to the shards.
SET enable_parallel_replicas = 0;
SET serialize_query_plan = 0;

SELECT count()
FROM
(
    SELECT number AS x FROM remote('127.0.0.{1,2}', numbers(100))
    UNION ALL
    SELECT number AS x FROM remote('127.0.0.{1,2}', numbers(100))
)
WHERE x GLOBAL IN (SELECT number FROM numbers(10))
SETTINGS prefer_localhost_replica = 0;

SELECT count()
FROM
(
    SELECT number AS x FROM remote('127.0.0.{1,2}', numbers(100))
    UNION ALL
    SELECT number AS x FROM remote('127.0.0.{1,2}', numbers(100))
)
WHERE x GLOBAL IN (SELECT number FROM numbers(10))
SETTINGS prefer_localhost_replica = 1;

DROP TABLE IF EXISTS t_global_in_union;
CREATE TABLE t_global_in_union (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 0;
SYSTEM STOP MERGES t_global_in_union;
INSERT INTO t_global_in_union SELECT number FROM numbers(100)
SETTINGS max_block_size = 10, min_insert_block_size_rows = 10, max_insert_threads = 1;

SELECT count()
FROM
(
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_global_in_union)
    UNION ALL
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_global_in_union)
)
WHERE x GLOBAL IN (SELECT number FROM numbers(10))
SETTINGS prefer_localhost_replica = 0;

-- The set's table attached by distributed index analysis of the local read is reused by the remote read of the other branch.
SELECT count()
FROM
(
    SELECT x FROM t_global_in_union
    UNION ALL
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_global_in_union)
)
WHERE x GLOBAL IN (SELECT number FROM numbers(10))
SETTINGS prefer_localhost_replica = 0, distributed_index_analysis = 1,
    distributed_index_analysis_for_non_shared_merge_tree = 1, distributed_index_analysis_only_on_coordinator = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas', use_statistics_for_part_pruning = 0;

DROP TABLE t_global_in_union;
