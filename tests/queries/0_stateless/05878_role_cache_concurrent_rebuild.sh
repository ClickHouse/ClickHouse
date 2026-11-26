#!/usr/bin/env bash
# Tags: race, no-fasttest
# Concurrently change a role that takes part in the users' enabled role sets while those users log in.
# The role cache recalculates the enabled sets off its lock, so this stresses the rebuild-vs-login path
# for TSan/ASan, then checks that grants and revokes (incl. on a newly granted nested role) still apply.
# Users and roles are scoped to this test, so parallel tests are unaffected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

R_BIG="r_big_${CLICKHOUSE_DATABASE}"
R_NESTED="r_nested_${CLICKHOUSE_DATABASE}"
USERS=()
for i in 1 2 3; do USERS+=("u${i}_${CLICKHOUSE_DATABASE}"); done

function cleanup()
{
    for i in 1 2 3; do
        $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS u${i}_${CLICKHOUSE_DATABASE}; DROP ROLE IF EXISTS ru${i}_${CLICKHOUSE_DATABASE}"
    done
    $CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS ${R_BIG}; DROP ROLE IF EXISTS ${R_NESTED}"
}

cleanup
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t (id UInt64, $(seq -f 'c%g String' 0 199 | paste -sd, -), toggle String, nested String, nested2 String) ENGINE = MergeTree ORDER BY id;
CREATE ROLE ${R_BIG};
GRANT SELECT(id, $(seq -f 'c%g' 0 199 | paste -sd, -)) ON ${CLICKHOUSE_DATABASE}.t TO ${R_BIG};
CREATE ROLE ${R_NESTED};
GRANT SELECT(nested) ON ${CLICKHOUSE_DATABASE}.t TO ${R_NESTED};
"
for i in 1 2 3; do
    $CLICKHOUSE_CLIENT -q "
    CREATE ROLE ru${i}_${CLICKHOUSE_DATABASE};
    GRANT SELECT(c0) ON ${CLICKHOUSE_DATABASE}.t TO ru${i}_${CLICKHOUSE_DATABASE};
    CREATE USER u${i}_${CLICKHOUSE_DATABASE};
    GRANT ${R_BIG}, ru${i}_${CLICKHOUSE_DATABASE} TO u${i}_${CLICKHOUSE_DATABASE};
    "
done

TIMEOUT=5

function toggle()
{
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT -q "GRANT SELECT(toggle) ON ${CLICKHOUSE_DATABASE}.t TO ${R_BIG}"
        $CLICKHOUSE_CLIENT -q "REVOKE SELECT(toggle) ON ${CLICKHOUSE_DATABASE}.t FROM ${R_BIG}"
        $CLICKHOUSE_CLIENT -q "GRANT ${R_NESTED} TO ${R_BIG}"
        $CLICKHOUSE_CLIENT -q "REVOKE ${R_NESTED} FROM ${R_BIG}"
    done
}

function login()
{
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        for user in "${USERS[@]}"; do
            res=$($CLICKHOUSE_CLIENT --user "${user}" -q "SELECT count() FROM t WHERE c0 = '' AND id = 0" 2>&1)
            [ "$res" = "0" ] || echo "FAIL: ${user}: $res"
        done
    done
}

toggle &
login &
login &
wait

# $1 = label, $2 = query, $3 = expected outcome (ok or denied); prints for how many users it held.
function check()
{
    local n=0
    for user in "${USERS[@]}"; do
        res=$($CLICKHOUSE_CLIENT --user "${user}" -q "$2" 2>&1)
        if [ "$3" = "ok" ]; then
            [ "$res" = "0" ] && n=$((n + 1))
        else
            [[ "$res" == *ACCESS_DENIED* ]] && n=$((n + 1))
        fi
    done
    echo "$1: $n"
}

$CLICKHOUSE_CLIENT -q "GRANT SELECT(toggle) ON ${CLICKHOUSE_DATABASE}.t TO ${R_BIG}; GRANT ${R_NESTED} TO ${R_BIG}"
check "granted" "SELECT count() FROM t WHERE toggle = '' AND nested = ''" ok

# A change to the role granted above must still reach the users' enabled sets.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(nested2) ON ${CLICKHOUSE_DATABASE}.t TO ${R_NESTED}"
check "nested2" "SELECT count() FROM t WHERE nested2 = ''" ok

$CLICKHOUSE_CLIENT -q "REVOKE SELECT(toggle) ON ${CLICKHOUSE_DATABASE}.t FROM ${R_BIG}; REVOKE ${R_NESTED} FROM ${R_BIG}"
check "revoked toggle" "SELECT count() FROM t WHERE toggle = ''" denied
check "revoked nested" "SELECT count() FROM t WHERE nested = ''" denied

cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

echo OK
