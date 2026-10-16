#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: the table engine is replaced.

# A detached partition of patch parts can be attached back when the database is at its `max_rows` limit:
# patch parts represent mutations of existing rows, not additional rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rows"

CH="${CLICKHOUSE_CLIENT} --insert_keeper_fault_injection_probability=0 --enable_lightweight_update=1"

$CH -q "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB} ENGINE = Atomic;
CREATE TABLE ${DB}.mt (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE ${DB}.rmt (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', 'r1') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES ${DB}.mt;
SYSTEM STOP MERGES ${DB}.rmt;
INSERT INTO ${DB}.mt SELECT number, 0 FROM numbers(10);
INSERT INTO ${DB}.rmt SELECT number, 0 FROM numbers(10);
UPDATE ${DB}.mt SET v = 1 WHERE id < 4;
UPDATE ${DB}.rmt SET v = 1 WHERE id < 4;
ALTER DATABASE ${DB} MODIFY SETTING max_rows = 20;
"

for table in mt rmt; do
    patch_partition_id=$($CH -q "
        SELECT any(partition_id) FROM system.parts
        WHERE database = '${DB}' AND table = '${table}' AND active AND startsWith(partition_id, 'patch-')")

    $CH -q "ALTER TABLE ${DB}.${table} DETACH PARTITION ID '${patch_partition_id}'"
    $CH -q "ALTER TABLE ${DB}.${table} ATTACH PARTITION ID '${patch_partition_id}'"
    $CH -q "SELECT '${table}', count(), countIf(v = 1) FROM ${DB}.${table}"
done

$CH -q "SELECT rows FROM system.databases WHERE name = '${DB}'"

$CH -q "DROP DATABASE ${DB}"
