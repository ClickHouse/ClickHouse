#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-fasttest
# Tag no-parallel: the failpoint pauses the first replicated DDL entry that any query on the server executes.
# Tag no-replicated-database: the test creates its own Replicated database.
# Tag no-fasttest: Fast test has no Keeper.

# SYSTEM RESTORE DATABASE REPLICA replaces the database's DDL worker, so it must wait for a
# replicated DDL entry that a query is still executing through the current worker.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rdb"
ZK_PATH="/test/05442_restore_database_replica/${CLICKHOUSE_TEST_UNIQUE_NAME}"
FAIL_POINT="database_replicated_pause_before_initial_entry_execution"
OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${FAIL_POINT}"
    wait
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"
    rm -f "${OUT}".*
}
trap cleanup EXIT

function replica_exists()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.zookeeper WHERE path = '${ZK_PATH}/replicas' AND name = 's1|r1'"
}

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"
${CLIENT} -q "CREATE TABLE ${DB}.seed (n UInt64) ENGINE = MergeTree ORDER BY n"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT ${FAIL_POINT}"

# Parks inside tryEnqueueAndExecuteEntry, still holding its table DDLGuard and so the shared database lock.
(${CLIENT} -q "CREATE TABLE ${DB}.parked (n UInt64) ENGINE = MergeTree ORDER BY n" > "${OUT}.create" 2>&1; echo $? > "${OUT}.create_rc") &
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT ${FAIL_POINT} PAUSE"

# A healthy replica refuses the restore, so drop this replica's Keeper state. /log survives, so the
# parked entry can still commit.
for _ in {1..10}; do
    ${CLICKHOUSE_KEEPER_CLIENT} -q "rmr '${ZK_PATH}/replicas/s1|r1'" > /dev/null 2>&1
    [ "$(replica_exists)" = "0" ] && break
    sleep 0.5
done
replica_exists

(${CLICKHOUSE_CLIENT} -q "SYSTEM RESTORE DATABASE REPLICA ${DB}" > "${OUT}.restore" 2>&1; echo $? > "${OUT}.restore_rc") &

# Once the restore waits for the exclusive database lock, every other DDL on the database is refused,
# so a no-op DROP is a latch for it. Until then the restore must neither finish nor recreate the replica.
blocked=0
for _ in {1..300}; do
    if [ -s "${OUT}.restore_rc" ] || [ "$(replica_exists)" != "0" ]; then
        echo "SYSTEM RESTORE DATABASE REPLICA went ahead while a replicated DDL entry was still executing"
        break
    fi
    if ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${DB}.lock_probe" 2>&1 | grep -q -F "Unable to acquire the database lock"; then
        blocked=1
        break
    fi
    sleep 0.1
done
echo "${blocked}"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT ${FAIL_POINT}"
wait

cat "${OUT}.restore_rc"
[ "$(cat "${OUT}.restore_rc")" = "0" ] || cat "${OUT}.restore"
# The released entry loses the race for the database lock the restore now holds, as it does against DROP DATABASE.
if [ "$(cat "${OUT}.create_rc")" != "0" ] && grep -q -F "Code: 159" "${OUT}.create" && grep -q -F "Unable to acquire the database lock" "${OUT}.create"; then
    echo 1
else
    cat "${OUT}.create"
fi
replica_exists
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.seed"
