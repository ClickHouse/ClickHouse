#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide
# no-replicated-database: the test removes a znode of the table by its explicit path
# no-shared-merge-tree: SharedMergeTree has no unexpected parts

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_load_unexpected_parts_cannot_schedule"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_load_unexpected_parts_cannot_schedule SYNC;
    CREATE TABLE t_load_unexpected_parts_cannot_schedule (x UInt64)
        ENGINE = ReplicatedMergeTree('$ZK_PATH', 'r1') ORDER BY x;
    SYSTEM STOP MERGES t_load_unexpected_parts_cannot_schedule;
    INSERT INTO t_load_unexpected_parts_cannot_schedule VALUES (1);
    INSERT INTO t_load_unexpected_parts_cannot_schedule VALUES (2);
    DETACH TABLE t_load_unexpected_parts_cannot_schedule;
"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$ZK_PATH/replicas/r1/parts/all_1_1_0'"

$CLICKHOUSE_CLIENT -q "
    SYSTEM ENABLE FAILPOINT merge_tree_load_unexpected_parts_cannot_schedule;
    ATTACH TABLE t_load_unexpected_parts_cannot_schedule;
    SELECT x FROM t_load_unexpected_parts_cannot_schedule ORDER BY x;
    SELECT name, reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_load_unexpected_parts_cannot_schedule';
    DROP TABLE t_load_unexpected_parts_cannot_schedule SYNC;
"
