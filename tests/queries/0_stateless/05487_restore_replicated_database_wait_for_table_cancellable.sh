#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: uses a global one-shot failpoint in `DatabaseReplicated::createTableRestoredFromBackup`.

# A `RESTORE` into a `Replicated` database waits until the restored table appears,
# if another replica has acquired its creation. The wait must be cancellable:
# here it is interrupted by `max_execution_time` instead of waiting for `backups.create_table_timeout` (300 s).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rdb"

# `Memory` backups live in the session, so everything runs in a single client session.
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode none -m -q "
    SYSTEM DISABLE FAILPOINT database_replicated_restore_table_created_by_other_replica;
    DROP DATABASE IF EXISTS ${DB} SYNC;
    CREATE DATABASE ${DB} ENGINE = Replicated('/test/databases/${DB}/restore_wait_cancellable', 's1', 'r1');
    CREATE TABLE ${DB}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${DB}.src VALUES (1), (2);
    BACKUP TABLE ${DB}.src TO Memory('b') FORMAT Null;

    SYSTEM ENABLE FAILPOINT database_replicated_restore_table_created_by_other_replica;
    RESTORE TABLE ${DB}.src AS ${DB}.dst FROM Memory('b') SETTINGS max_execution_time = 3 FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }
    SYSTEM DISABLE FAILPOINT database_replicated_restore_table_created_by_other_replica;

    -- Without the failpoint the restore works.
    RESTORE TABLE ${DB}.src AS ${DB}.dst2 FROM Memory('b') FORMAT Null;
    SELECT count() FROM ${DB}.dst2;

    DROP DATABASE ${DB} SYNC;
"
