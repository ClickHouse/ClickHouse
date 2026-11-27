#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel -- uses server-wide failpoints that would break the persistent `Set` inserts of
# concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Set` replays its backup into the live `Set`, and a failed replay is
# rolled back by rebuilding the state from the other committed backups. If the rebuild fails as well,
# the `Set` still has rows of the failed insert. It must not be used then: queries get an exception
# until the next `INSERT` rebuilds the state.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS set_lost_state;
    CREATE TABLE set_lost_state (k UInt64) ENGINE = Set SETTINGS persistent = 1;
    INSERT INTO set_lost_state VALUES (1);
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_fail_during_backup_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_publish_fail_during_rollback"
$CLICKHOUSE_CLIENT --query "INSERT INTO set_lost_state VALUES (2), (3)" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_fail_during_backup_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_publish_fail_during_rollback"

echo "query after the failed rollback:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_lost_state" 2>&1 | grep -o 'NOT_INITIALIZED' | head -n 1

# The next insert rebuilds the state from the committed backups before adding its own rows.
$CLICKHOUSE_CLIENT --query "INSERT INTO set_lost_state VALUES (4)"
echo "rows after the next insert:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_lost_state"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE set_lost_state;
    ATTACH TABLE set_lost_state;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_lost_state"

$CLICKHOUSE_CLIENT --query "DROP TABLE set_lost_state"
