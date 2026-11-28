#!/usr/bin/env bash

# A grant on a name that looks like a subcolumn of an existing column (`secret.no_such_subcolumn`) must not make the
# table metadata visible to the analyzer when the column has no such subcolumn: `EXPLAIN QUERY TREE` would otherwise
# reveal the columns and types of a table the user cannot `DESCRIBE`. A grant on a real subcolumn, including a dynamic
# path of a `JSON` column, still makes it visible.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05490_${CLICKHOUSE_DATABASE}"
t="${CLICKHOUSE_DATABASE}.t"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE t (secret UInt64, tup Tuple(a UInt64), j JSON) ENGINE = MergeTree ORDER BY tuple();
CREATE USER $user;
"

check()
{
    local out
    $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user; GRANT $1 ON $t TO $user"
    if out=$($CLICKHOUSE_CLIENT --user "$user" -q "EXPLAIN QUERY TREE SELECT * FROM $t" 2>&1); then
        echo "$1: visible"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then
        echo "$1: ACCESS_DENIED"
    else
        echo "$1: $out"
    fi
}

check "SELECT(\`secret.no_such_subcolumn\`)"
check "SHOW COLUMNS(\`secret.no_such_subcolumn\`)"
check "SELECT(\`tup.no_such_subcolumn\`)"
check "SELECT(\`no_such_column.a\`)"
check "SELECT(\`tup.a\`)"
check "SHOW COLUMNS(\`tup.a\`)"
check "SELECT(\`j.some.path\`)"

$CLICKHOUSE_CLIENT -q "
DROP USER $user;
DROP TABLE t;
"
