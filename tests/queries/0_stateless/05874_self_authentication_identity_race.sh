#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: user manipulation is not supported there
# Tag no-parallel: failpoints are server-wide, and other tests execute self-service `ALTER USER`

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user="self_auth_race_${CLICKHOUSE_DATABASE}"
renamed="${user}_renamed"
password="InitialPassword_05874"
replacement_password="ReplacementPassword_05874"
attempted_password="DeniedPassword_05874"
output_file="${CLICKHOUSE_TMP}/self_auth_race_${CLICKHOUSE_DATABASE}.out"

admin()
{
    $CLICKHOUSE_CLIENT --multiquery --query "$1"
}

cleanup()
{
    admin "SYSTEM DISABLE FAILPOINT self_authentication_before_identity_lookup;
           SYSTEM DISABLE FAILPOINT self_authentication_before_update;
           DROP USER IF EXISTS $user, $renamed;" >/dev/null
}
trap cleanup EXIT

fail()
{
    echo "$*" >&2
    exit 1
}

# Pauses a self-service password change at the failpoint, applies the concurrent change,
# resumes it, and checks its outcome and that no surviving account was updated.
run_case()
{
    local failpoint="$1" change="$2" if_exists="$3" expected="$4"
    local survivors=() definitions=() pid status output i name secret

    cleanup
    admin "CREATE USER $user IDENTIFIED WITH sha256_password BY '$password';
           GRANT ALTER USER SELF AUTHENTICATION TO $user;
           SYSTEM ENABLE FAILPOINT $failpoint;"

    $CLICKHOUSE_CLIENT --user "$user" --password "$password" \
        --query "ALTER USER $if_exists $user IDENTIFIED WITH sha256_password BY '$attempted_password'" \
        >"$output_file" 2>&1 &
    pid=$!
    admin "SYSTEM WAIT FAILPOINT $failpoint PAUSE"

    if [[ "$change" == rename_replace ]]; then
        admin "ALTER USER $user RENAME TO $renamed"
        survivors+=("$renamed:$password")
    else
        admin "DROP USER $user"
    fi
    admin "CREATE USER $user IDENTIFIED WITH sha256_password BY '$replacement_password'"
    survivors+=("$user:$replacement_password")
    for i in "${!survivors[@]}"; do
        definitions[i]=$(admin "SHOW CREATE USER ${survivors[i]%%:*}")
    done

    admin "SYSTEM DISABLE FAILPOINT $failpoint"
    status=0
    wait "$pid" || status=$?
    output=$(cat "$output_file")
    if [[ -z "$expected" ]]; then
        [[ "$status" == 0 && -z "$output" ]] || fail "$failpoint/$change: expected success, received: $output"
    else
        [[ "$status" != 0 && "$output" == *"(ACCESS_DENIED)"* && "$output" == *"$expected"* ]] \
            || fail "$failpoint/$change: expected '$expected', received: $output"
    fi

    for i in "${!survivors[@]}"; do
        name="${survivors[i]%%:*}"
        secret="${survivors[i]#*:}"
        [[ "$(admin "SHOW CREATE USER $name")" == "${definitions[i]}" ]] || fail "$failpoint/$change: $name was changed"
        [[ "$($CLICKHOUSE_CLIENT --user "$name" --password "$secret" --query "SELECT 1")" == 1 ]] \
            || fail "$failpoint/$change: $name lost its password"
        if $CLICKHOUSE_CLIENT --user "$name" --password "$attempted_password" --query "SELECT 1" >/dev/null 2>&1; then
            fail "$failpoint/$change: $name accepts the attempted password"
        fi
    done
}

# The account was recreated before lookup: the name resolves to another ID.
run_case self_authentication_before_identity_lookup drop_replace "IF EXISTS" \
    "Self-service authentication requires the authenticated account"
echo "Recreated account is rejected at lookup"

# The bound account was renamed before the update: it must not be renamed back.
run_case self_authentication_before_update rename_replace "" \
    "Self-service authentication target changed during alteration"
echo "Renamed account is rejected at update"

# The bound account was recreated before the update: the replacement is not updated.
run_case self_authentication_before_update drop_replace "IF EXISTS" ""
echo "Recreated account is skipped at update"
