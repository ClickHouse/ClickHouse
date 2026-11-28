#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of the engine `Alias` has the metadata of its target. `cols` and `meta` of the `q` dialect read the metadata
# and generate queries of literals, so nothing checks the grants on the target later: the columns must be seen
# through the grants on the target as well, as in `system.columns`.

user="u_05885_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    SET allow_experimental_alias_table_engine = 1;
    CREATE TABLE target (a Int32, hidden String) ENGINE = Memory;
    CREATE TABLE shortcut ENGINE = Alias('target');
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.shortcut TO ${user};
"

function q()
{
    $CLICKHOUSE_CLIENT --user "${user}" --enable_q_dialect 1 --dialect q -q "$1" 2>&1 | grep -o -m1 "ACCESS_DENIED\|^\[.*\]$" | head -n 1
}

echo "--- a grant on the alias alone"
q "cols shortcut"
q "meta shortcut"

echo "--- and a grant on one column of the target"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS(a) ON ${CLICKHOUSE_DATABASE}.target TO ${user}"
q "cols shortcut"

echo "--- and a grant on the target"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.target TO ${user}"
q "cols shortcut"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
