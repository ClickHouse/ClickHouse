#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: restoring `system.functions` replaces the server-global set of SQL user-defined functions.

# `RESTORE TABLE system.functions ... SETTINGS create_function = 'replace'` replaces an existing
# function, so it needs `DROP FUNCTION` on top of `CREATE FUNCTION`, like `CREATE OR REPLACE FUNCTION`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05873_${CLICKHOUSE_DATABASE}"
func="func_05873_${CLICKHOUSE_DATABASE}"
bk="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b')"

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS $func"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --multiquery -q "
CREATE FUNCTION $func AS (x) -> x + 1;
CREATE USER $user;
GRANT BACKUP ON system.functions TO $user;
GRANT CREATE FUNCTION ON *.* TO $user;
GRANT READ ON FILE TO $user;
"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE system.functions TO $bk FORMAT Null"

restore() { ${CLICKHOUSE_CLIENT} --user "$user" -q "RESTORE TABLE system.functions FROM $bk SETTINGS $1 FORMAT Null" 2>&1; }

echo "-- replace mode without DROP FUNCTION: denied, and it names the missing grant"
restore "create_function = 'replace'" | grep -c -m1 'DROP FUNCTION'

echo "-- replace mode with DROP FUNCTION: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT DROP FUNCTION ON *.* TO $user"
if restore "create_function = 'replace'" | grep -q 'ACCESS_DENIED'; then echo "denied"; else echo "allowed"; fi

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS $func"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
