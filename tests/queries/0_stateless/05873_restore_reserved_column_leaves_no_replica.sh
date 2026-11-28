#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database
# no-replicated-database: the table uses an explicit Keeper path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `RESTORE` rejected for a reserved column name must not leave a replica in Keeper, so that a retry succeeds.

BACKUP_ID="${CLICKHOUSE_DATABASE}_05873"

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t (x UInt64, _part String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05873_t', 'r1') ORDER BY x
    SETTINGS allow_reserved_column_names = 1;
INSERT INTO t (x, _part) VALUES (1, 'a');
BACKUP TABLE t TO Disk('backups', '${BACKUP_ID}') FORMAT Null;
DROP TABLE t SYNC;

RESTORE TABLE t FROM Disk('backups', '${BACKUP_ID}') SETTINGS allow_reserved_column_names = 0 FORMAT Null; -- { serverError ILLEGAL_COLUMN }
RESTORE TABLE t FROM Disk('backups', '${BACKUP_ID}') SETTINGS allow_reserved_column_names = 1 FORMAT Null;
SELECT x FROM t;
DROP TABLE t SYNC;
"
