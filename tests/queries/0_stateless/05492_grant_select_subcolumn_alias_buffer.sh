#!/usr/bin/env bash

# `Alias` and `Buffer` forward a read to their target table and check the column-level `SELECT` grants on it there.
# The check must resolve a subcolumn to its storage column, as the check on a plain table does, and it must not
# reach the target of a user who has no grant on it at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05492_${CLICKHOUSE_DATABASE}"
db="$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE dst (k UInt64, t Tuple(a UInt8, b String), m Map(String, UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO dst VALUES (1, (7, 'x'), {'p': 1});
SET allow_experimental_alias_table_engine = 1;
CREATE TABLE al ENGINE = Alias('$db', 'dst');
CREATE TABLE buf (k UInt64, t Tuple(a UInt8, b String), m Map(String, UInt8))
    ENGINE = Buffer('$db', dst, 1, 10, 100, 10000, 1000000, 10000000, 100000000);
CREATE USER $user;
GRANT SELECT ON $db.al TO $user;
GRANT SELECT ON $db.buf TO $user;
"

function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user" -q "$1" 2>&1); then
        echo "OK: $output"
    elif grep -q "ACCESS_DENIED" <<< "$output"; then
        echo "ACCESS_DENIED"
    else
        echo "$output" | grep -o 'Code: [0-9]*' | head -n1
    fi
}

echo "-- no grants on the target: denied"
run_as_user "SELECT t.a FROM al"
run_as_user "SELECT t.a FROM buf"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(t) ON $db.dst TO $user"
echo "-- a grant on the column covers its subcolumns, the other column stays denied"
run_as_user "SELECT t.a, t.b FROM al"
run_as_user "SELECT t.a, t.b FROM buf"
run_as_user "SELECT m.keys FROM al"
run_as_user "SELECT m.keys FROM buf"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.dst TO $user; REVOKE SELECT(m) ON $db.dst FROM $user"
echo "-- a revoked column denies its subcolumns, the rest is allowed"
run_as_user "SELECT m.keys FROM al"
run_as_user "SELECT m.keys FROM buf"
run_as_user "SELECT k, t.a FROM al"
run_as_user "SELECT k, t.a FROM buf"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
