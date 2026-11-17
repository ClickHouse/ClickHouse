#!/usr/bin/env bash
# `ALTER DATABASE`, `KILL TRANSACTION` and `INSERT INTO FUNCTION` must be refused in readonly mode
# (including HTTP GET, which implies `readonly = 2`), and `ALTER DATABASE` also with `allow_ddl = 0`.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE
file="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
trap 'rm -f "${USER_FILES_PATH:?}/$file"' EXIT

# Prints the error code of a refused statement, or `accepted`.
outcome()
{
    local out rc code
    out=$("$@" 2>&1); rc=$?
    code=$(grep -o -m1 -E '\. \([A-Z_]+\)' <<< "$out" | head -n1 | tr -d ' .()')
    if [ -n "$code" ]; then echo "$code"; elif [ "$rc" -eq 0 ]; then echo 'accepted'; else echo "failed: $out"; fi
}
get() { ${CLICKHOUSE_CURL} -sS --get "${CLICKHOUSE_URL}" --data-urlencode "query=$1"; }

# Each statement is accepted without readonly.
echo "alter database comment: $(outcome $CLICKHOUSE_CLIENT -q "ALTER DATABASE $db MODIFY COMMENT 'kept'")"
echo "insert into function: $(outcome $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$file', 'CSV', 'x Int32') SELECT 1 SETTINGS engine_file_truncate_on_insert = 1")"
echo "kill transaction: $(outcome $CLICKHOUSE_CLIENT -q "KILL TRANSACTION WHERE tid_hash = 0")"

for setting in "--readonly 1" "--readonly 2" "--allow_ddl 0"; do
    echo "$setting"
    echo "select: $(outcome $CLICKHOUSE_CLIENT $setting -q "SELECT 1 FORMAT Null")"
    echo "alter database comment: $(outcome $CLICKHOUSE_CLIENT $setting -q "ALTER DATABASE $db MODIFY COMMENT 'changed'")"
    echo "alter database setting: $(outcome $CLICKHOUSE_CLIENT $setting -q "ALTER DATABASE $db MODIFY SETTING max_tables = 100000")"
    [ "$setting" = "--allow_ddl 0" ] && continue
    echo "kill transaction: $(outcome $CLICKHOUSE_CLIENT $setting -q "KILL TRANSACTION WHERE tid_hash = 0")"
    echo "insert into function: $(outcome $CLICKHOUSE_CLIENT $setting -q "INSERT INTO FUNCTION file('$file', 'CSV', 'x Int32') SELECT 2 SETTINGS engine_file_truncate_on_insert = 1")"
    echo "insert into function cluster: $(outcome $CLICKHOUSE_CLIENT $setting -q "INSERT INTO FUNCTION cluster('05816_no_such_cluster', system.one) SELECT 1")"
done

echo "HTTP GET"
echo "alter database comment: $(outcome get "ALTER DATABASE $db MODIFY COMMENT 'changed'")"
response=$(get "INSERT INTO FUNCTION file('$file', 'CSV', 'x Int32') SELECT 3 SETTINGS engine_file_truncate_on_insert = 1")
echo "insert into function: $(outcome printf '%s' "$response")"
echo "$response" | grep -o -m1 'method GET implies readonly'

# Nothing was changed.
$CLICKHOUSE_CLIENT -q "SELECT comment FROM system.databases WHERE name = '$db'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('$file', 'CSV', 'x Int32')"
