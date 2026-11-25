#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree
# Global failpoint; this checks the plain `MergeTree` mutation registration path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_pending_registration"
}
trap cleanup EXIT

for column_kind in 'EPHEMERAL' 'ALIAS c'; do
    $CLICKHOUSE_CLIENT --multiquery -q "
        DROP TABLE IF EXISTS t_pending_registration;
        CREATE TABLE t_pending_registration (a UInt64, c UInt64)
        ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO t_pending_registration VALUES (7, 0), (9, 0);
        SYSTEM STOP MERGES t_pending_registration;
        SYSTEM ENABLE FAILPOINT mt_pause_before_register_mutation;"

    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_pending_registration UPDATE c = a WHERE 1 SETTINGS mutations_sync = 0" &
    mutation_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_register_mutation PAUSE"

    # The earlier mutation has a version but is not visible in `system.mutations` yet.
    # Conversion must time out on the `ALTER` lock held through mutation registration.
    if result=$($CLICKHOUSE_CLIENT --lock_acquire_timeout 1 -q "ALTER TABLE t_pending_registration MODIFY COLUMN a UInt64 $column_kind" 2>&1); then
        echo "conversion overtook an unregistered mutation"
        exit 1
    fi
    if ! echo "$result" | grep -o -m1 DEADLOCK_AVOIDED; then
        printf '%s\n' "$result" >&2
        exit 1
    fi
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_pending_registration' AND name = 'a' AND default_kind = ''"

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_register_mutation"
    wait "$mutation_pid"
    # Once registered, an unfinished consumer must cause the usual immediate rejection.
    if result=$($CLICKHOUSE_CLIENT -q "ALTER TABLE t_pending_registration MODIFY COLUMN a UInt64 $column_kind" 2>&1); then
        echo "conversion overtook a registered mutation"
        exit 1
    fi
    if ! echo "$result" | grep -o -m1 BAD_ARGUMENTS; then
        printf '%s\n' "$result" >&2
        exit 1
    fi

    $CLICKHOUSE_CLIENT --multiquery -q "
        SYSTEM START MERGES t_pending_registration;
        ALTER TABLE t_pending_registration UPDATE c = c WHERE 1 SETTINGS mutations_sync = 2;
        ALTER TABLE t_pending_registration MODIFY COLUMN a UInt64 $column_kind;
        SELECT sum(c) FROM t_pending_registration;
        DROP TABLE t_pending_registration;"
done
