#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3

# https://github.com/ClickHouse/ClickHouse/issues/128299
#
# An `INSERT ... SELECT FROM s3Cluster(...)` that inserts on the initiator must fill the fields missing in the files
# with the `DEFAULT` expressions of the destination table, the same as `s3(...)`. The initiator takes the columns of
# the source from the insertion table, but sent them to the nodes as names and types only, so the nodes filled the
# missing fields with the default values of their types (0, 1970-01-01).
#
# All nodes of the clusters below are this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05474"

# The first file has none of the defaulted fields, the second one has all of them.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('${S3_DIR}/part_1.jsonl', 'JSONEachRow', 'id UInt32') VALUES (1)"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3('${S3_DIR}/part_2.jsonl', 'JSONEachRow', 'id UInt32, d Date, q UInt32, e UInt32') VALUES (2, '2001-02-03', 5, 6)"

S3_CLUSTER="s3Cluster('test_cluster_one_shard_three_replicas_localhost', '${S3_DIR}/part_{1,2}.jsonl', 'JSONEachRow')"

# The first INSERT goes into a `Distributed` table with a deterministic sharding key, which makes the distributed INSERT
# fall back to the ordinary one, inserting on the initiator. `parallel_distributed_insert_select = 2` is the default,
# which the test runner can randomize, and `distributed_foreground_insert = 1` makes the rows reach `dst_05474` before
# the INSERT returns.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dst_05474 (id UInt32, d Date DEFAULT '2000-01-01', q UInt32 DEFAULT 7, e UInt32 DEFAULT id * 10)
    ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dist_05474 AS dst_05474 ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), dst_05474, id);

    INSERT INTO dist_05474 SELECT * FROM ${S3_CLUSTER} SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
    SELECT '--- Distributed with a deterministic sharding key ---';
    SELECT * FROM dst_05474 ORDER BY id;
    TRUNCATE TABLE dst_05474;

    INSERT INTO dst_05474 SELECT * FROM ${S3_CLUSTER} SETTINGS parallel_distributed_insert_select = 0;
    SELECT '--- parallel_distributed_insert_select = 0 ---';
    SELECT * FROM dst_05474 ORDER BY id;

    DROP TABLE dist_05474;
    DROP TABLE dst_05474;
"
