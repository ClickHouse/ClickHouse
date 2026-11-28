#!/usr/bin/env bash
# Tags: no-fasttest
# ^ backups need a running server with a configured 'backups' disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# backup_use_packed_format rejects the combinations it has no path for, before writing anything.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

small_create="CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0"
small_insert="INSERT INTO t SELECT number, toString(number) FROM numbers(50)"

${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"

# Case 1: packing is mutually exclusive with an archive destination.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_1.zip') SETTINGS backup_use_packed_format=1" 2>&1 \
    | grep -o "is not supported with an archive destination" | head -1

# Case 2: deduplicate_files=0 has no dedup identity to pack by.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_2') SETTINGS backup_use_packed_format=1, deduplicate_files=0" 2>&1 \
    | grep -o "not supported with deduplicate_files" | head -1

# Case 3: ON CLUSTER coordination does not pass the packing settings on, so packing would silently do nothing.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t ON CLUSTER test_shard_localhost TO Disk('backups', '${name}_3') SETTINGS backup_use_packed_format=1" 2>&1 \
    | grep -o "not supported with ON CLUSTER" | head -1

# Case 4: a lightweight snapshot stores files by object key, which packing has no path for.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_4') SETTINGS backup_use_packed_format=1, experimental_lightweight_snapshot=1" 2>&1 \
    | grep -o "not supported with experimental_lightweight_snapshot" | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
