#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM DROP DATABASE REPLICA ... WITH TABLES` creates the tables of a `Replicated` database from their definitions stored in
# Keeper, to drop the replica of each table. Such a definition describes a table that exists, so the check that rejects a key
# computed from an aggregated column of `AggregatingMergeTree` is not applied to it, even when the table no longer opts out of it.

DB="${CLICKHOUSE_DATABASE}_r1"
ZK_PATH="/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05468_rdb"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB SYNC"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -mn -q "
    CREATE DATABASE $DB ENGINE = Replicated('$ZK_PATH', 's1', 'r1');
    CREATE TABLE $DB.t_stored (c0 SimpleAggregateFunction(sum, Int64)) ENGINE = ReplicatedAggregatingMergeTree
    ORDER BY intDiv(c0, 55) SETTINGS allow_aggregating_columns_in_partition_or_order_key = 1;
    ALTER TABLE $DB.t_stored MODIFY SETTING allow_aggregating_columns_in_partition_or_order_key = 0;"

# A stale replica r2 that is left in Keeper. The replica r1 is dropped, so that the tables created from the stored definitions do
# not collide with its tables, which have the same UUIDs.
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$ZK_PATH/replicas', 's1|r2', '')"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB SYNC"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP DATABASE REPLICA 'r2' FROM SHARD 's1' FROM ZKPATH '$ZK_PATH' WITH TABLES"
$CLICKHOUSE_CLIENT -q "SELECT 'replicas left', count() FROM system.zookeeper WHERE path = '$ZK_PATH/replicas'"
