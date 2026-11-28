#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-ordinary-database, no-parallel
# Tag no-replicated-database: the test creates its own `Replicated` database and drops its replica.
# Tag no-shared-merge-tree: the table engine is replaced.
# Tag no-ordinary-database: tables of a `Replicated` database are restored by their UUID.
# Tag no-parallel: `SYSTEM DROP DATABASE REPLICA ... FROM ZKPATH` inspects the metadata of all databases of the server.

# `SYSTEM DROP DATABASE REPLICA ... FROM ZKPATH ... WITH TABLES` re-creates the tables of the dropped replica
# from the definitions stored in Keeper. `force_primary_key_reverse_order` must not rewrite those definitions,
# otherwise they do not match the metadata of the existing tables in Keeper (`METADATA_MISMATCH`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_repl"
DB_ZK_PATH="/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/05832_db"

$CLICKHOUSE_CLIENT --force_primary_key_reverse_order=0 -q "
    DROP DATABASE IF EXISTS $DB SYNC;
    CREATE DATABASE $DB ENGINE = Replicated('$DB_ZK_PATH', 's1', 'r1');
    CREATE TABLE $DB.t (k UInt64) ENGINE = ReplicatedMergeTree ORDER BY k;
" > /dev/null

TABLE_ZK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path FROM system.replicas WHERE database = '$DB' AND table = 't'")

# Another replica of the table keeps its metadata in Keeper after the database is dropped.
$CLICKHOUSE_CLIENT --force_primary_key_reverse_order=0 -q "
    -- The path is the default one of the table above. It contains the UUID of the table, so it is unique
    -- and does not need the prefix, which is mentioned only as the name of this replica.
    CREATE TABLE t_keep (k UInt64) ENGINE = ReplicatedMergeTree('$TABLE_ZK_PATH', 'r_keep_$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX') ORDER BY k;
"

# A replica of the database on another server, which has gone away.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO system.zookeeper (path, name, value)
    VALUES ('$DB_ZK_PATH/replicas', 's1|r2', 'gone:9000:00000000-0000-0000-0000-000000000000');
"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB SYNC"

$CLICKHOUSE_CLIENT --force_primary_key_reverse_order=1 -q "
    SYSTEM DROP DATABASE REPLICA 'r2' FROM SHARD 's1' FROM ZKPATH '$DB_ZK_PATH' WITH TABLES;
"

$CLICKHOUSE_CLIENT -q "SELECT 'database replicas', count() FROM system.zookeeper WHERE path = '$DB_ZK_PATH/replicas'"
$CLICKHOUSE_CLIENT -q "SELECT 'table replicas', groupArray(replaceOne(name, '_$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '')) FROM system.zookeeper WHERE path = '$TABLE_ZK_PATH/replicas'"
$CLICKHOUSE_CLIENT -q "SELECT 'sorting key', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_keep'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_keep SYNC"
