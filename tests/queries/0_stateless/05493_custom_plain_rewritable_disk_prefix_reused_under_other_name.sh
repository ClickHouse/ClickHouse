#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# no-fasttest: requires S3
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --write_through_distributed_cache 0 --read_through_distributed_cache 0 --multiline"

# A custom `plain_rewritable` disk is unregistered when the last table using it is dropped, and then shut down in the
# background. A disk with another name over the same object storage prefix is created only after the old one is shut
# down, so that the old one does not overwrite the metadata of the new one. While a table still uses a disk, another
# writable `plain_rewritable` disk over the same prefix is rejected.
endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}_plain_rewritable/"
first_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_first"
second_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_second"
third_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_third"

disk_definition()
{
    echo "disk(name = '$1', type = 's3_plain_rewritable', endpoint = '$endpoint', access_key_id = clickhouse, secret_access_key = clickhouse)"
}

$CLIENT -q "
DROP TABLE IF EXISTS t_first SYNC;
DROP TABLE IF EXISTS t_second SYNC;
DROP TABLE IF EXISTS t_third SYNC;

CREATE TABLE t_first (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS disk = $(disk_definition "$first_disk");
INSERT INTO t_first SELECT number FROM numbers(5);
SELECT 'first', count() FROM t_first;
DROP TABLE t_first SYNC;

CREATE TABLE t_second (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS disk = $(disk_definition "$second_disk");
INSERT INTO t_second SELECT number FROM numbers(7);
SELECT 'second', count() FROM t_second;
SELECT 'disks', groupArray(replaceOne(name, '$CLICKHOUSE_TEST_UNIQUE_NAME', '')) FROM system.disks WHERE name IN ('$first_disk', '$second_disk', '$third_disk');
"

$CLIENT -q "CREATE TABLE t_third (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS disk = $(disk_definition "$third_disk")" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

$CLIENT -q "
DROP TABLE t_second SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name IN ('$first_disk', '$second_disk', '$third_disk');
"
