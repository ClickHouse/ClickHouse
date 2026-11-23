#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `q` dialect respects column-level grants: a program needs the grants only for the columns it uses.

user="u_05873_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    CREATE TABLE t (a Int64, b String) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t VALUES (1, 'x'), (2, 'y');
    CREATE TABLE hidden (c Int64) ENGINE = MergeTree ORDER BY c;
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t TO ${user};
"

function q()
{
    local result
    result=$($CLICKHOUSE_CLIENT --user "${user}" --enable_q_dialect 1 --dialect q -q "$1" 2>&1)
    # Only the error code of an exception is printed.
    echo "${result}" | grep -o -m1 -E 'ACCESS_DENIED|UNKNOWN_IDENTIFIER' || echo "${result}"
}

echo "--- the granted column"
q 'select a from t'
q 'exec sum a from t'
q 'count select a from t'
echo "--- only the visible columns are known"
q 'cols t'
echo "--- the column without a grant"
q 'select b from t'
q 'select from t where b = `x'
echo "--- a table without grants"
q 'select from hidden'
q 'cols hidden'

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
