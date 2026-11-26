#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# A table naming the implicit storage policy `__<disk>` of a custom disk takes the registration of the disk
# together with the policy. If the policy does not exist yet, the table is rejected right away: otherwise
# a table defining the disk could create the policy before this table uses it, and this table would use
# the disk without keeping it registered.
disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"

$CLIENT -q "
DROP TABLE IF EXISTS test_defining SYNC;
DROP TABLE IF EXISTS test_policy SYNC;

CREATE TABLE test_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$disk'; -- { serverError UNKNOWN_POLICY }

CREATE TABLE test_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');

CREATE TABLE test_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$disk';

DROP TABLE test_defining SYNC;
SELECT 'disks after dropping the defining table', count() FROM system.disks WHERE name = '$disk';

INSERT INTO test_policy SELECT number FROM numbers(5);
SELECT 'rows', count() FROM test_policy;

DROP TABLE test_policy SYNC;
SELECT 'disks after dropping all tables', count() FROM system.disks WHERE name = '$disk';
"
