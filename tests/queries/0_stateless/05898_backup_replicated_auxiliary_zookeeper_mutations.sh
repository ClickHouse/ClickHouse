#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: shared merge tree doesn't support aux zookeepers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_path="${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Both tables use the same ZooKeeper path, but on different ZooKeeper clusters.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_default SYNC;
    DROP TABLE IF EXISTS t_aux SYNC;
    CREATE TABLE t_default (x UInt64, y UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY x;
    CREATE TABLE t_aux (x UInt64, y UInt64) ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/{database}/t', 'r2') ORDER BY x;
    INSERT INTO t_default VALUES (1, 1), (2, 2);
    INSERT INTO t_aux VALUES (1, 1), (2, 2);
    SYSTEM STOP MERGES t_default;
    SYSTEM STOP MERGES t_aux;
    ALTER TABLE t_default UPDATE y = y + 100 WHERE 1 SETTINGS mutations_sync = 0;
    ALTER TABLE t_aux UPDATE y = y + 10 WHERE 1 SETTINGS mutations_sync = 0;
    ALTER TABLE t_aux DELETE WHERE x = 2 SETTINGS mutations_sync = 0;
    BACKUP TABLE t_aux TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
"

find "${backup_path}/data/${CLICKHOUSE_DATABASE}/t_aux/mutations" -type f | sort | xargs grep -h '^commands'

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_default SYNC;
    DROP TABLE t_aux SYNC;
"
