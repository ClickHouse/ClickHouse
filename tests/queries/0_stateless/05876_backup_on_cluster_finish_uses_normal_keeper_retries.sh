#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the failpoint is global, so a concurrent `BACKUP` or `RESTORE ON CLUSTER` could consume it instead.

# A successful `BACKUP/RESTORE ... ON CLUSTER` must create its 'finish' nodes with the normal [Zoo]Keeper retries,
# not with `backup_restore_keeper_max_retries_while_handling_error`. The failpoint fails one [Zoo]Keeper request in `finish`,
# and with no retries while handling an error the operation succeeds only if `finish` retries it as usual.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

$CLICKHOUSE_CLIENT -m --query "
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(10);
"

$CLICKHOUSE_CLIENT -m --query "
    SYSTEM ENABLE FAILPOINT backup_coordination_finish_zk_fail_before_op;
    BACKUP TABLE ${CLICKHOUSE_DATABASE}.t ON CLUSTER test_shard_localhost TO $backup_name
    SETTINGS backup_restore_keeper_max_retries_while_handling_error = 0" | grep -o "BACKUP_CREATED"

$CLICKHOUSE_CLIENT -m --query "
    SYSTEM ENABLE FAILPOINT backup_coordination_finish_zk_fail_before_op;
    RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored ON CLUSTER test_shard_localhost FROM $backup_name
    SETTINGS backup_restore_keeper_max_retries_while_handling_error = 0" | grep -o "RESTORED"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT backup_coordination_finish_zk_fail_before_op"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"
