#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, distributed
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --distributed_foreground_insert 0 --multiline"

# A disk defined inline with `disk(...)` by a `MergeTree` table stays registered while a `Distributed` table
# uses its implicit storage policy `__<disk>`, also after the defining table is dropped. It is unregistered
# once the last of these tables is dropped.
# The disk is a plain `local` disk: `Distributed` writes its queue files into the directory of the disk directly,
# and on an object storage disk that is the metadata directory, where `system.remote_data_paths` of a concurrent
# test would read them as metadata.
disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"

$CLIENT -q "
DROP TABLE IF EXISTS test_defining SYNC;
DROP TABLE IF EXISTS test_target SYNC;
DROP TABLE IF EXISTS test_distributed SYNC;
DROP TABLE IF EXISTS test_distributed_2 SYNC;

CREATE TABLE test_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');

CREATE TABLE test_target (a Int32) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE test_distributed (a Int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_target, rand(), '__$disk');

DROP TABLE test_defining SYNC;
SELECT 'disks after the defining table is dropped', count() FROM system.disks WHERE name = '$disk';

INSERT INTO test_distributed SELECT number FROM numbers(3);
SYSTEM FLUSH DISTRIBUTED test_distributed;

CREATE TABLE test_distributed_2 (a Int32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_target, rand(), '__$disk');
DETACH TABLE test_distributed SYNC;
ATTACH TABLE test_distributed;

INSERT INTO test_distributed_2 SELECT number FROM numbers(4);
SYSTEM FLUSH DISTRIBUTED test_distributed_2;
SELECT 'rows in the target table', count() FROM test_target;

DROP TABLE test_distributed SYNC;
SELECT 'disks before the last table is dropped', count() FROM system.disks WHERE name = '$disk';
DROP TABLE test_distributed_2 SYNC;
SELECT 'disks after the last table is dropped', count() FROM system.disks WHERE name = '$disk';

DROP TABLE test_target SYNC;
"
