#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped

# `CREATE HYPOTHETICAL INDEX` and `EXPLAIN WHATIF` check `SELECT` on the columns the index reads the same way as a
# `SELECT` does: a subcolumn inherits the grant of its column, and a grant or revoke on the exact subcolumn name takes
# precedence over it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05454_${CLICKHOUSE_DATABASE}"
t="${CLICKHOUSE_DATABASE}.t"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE t (k UInt64, p Tuple(s UInt64, o UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 3;
INSERT INTO t SELECT number, (number, number) FROM numbers(12);
CREATE USER $user;
"

# Prints `+` or `-` for a hypothetical index on each of `p.s`, `p.o`, created and estimated by the user.
check()
{
    local line="$1:" out
    for column in p.s p.o; do
        if out=$($CLICKHOUSE_CLIENT --user "$user" -q "
            CREATE HYPOTHETICAL INDEX idx ON t ($column) TYPE minmax GRANULARITY 1;
            EXPLAIN WHATIF SELECT $column FROM t WHERE $column = 5;
        " 2>&1); then
            line="$line $column+"
        elif echo "$out" | grep -q 'ACCESS_DENIED'; then
            line="$line $column-"
        else
            line="$line $column:$out"
        fi
    done
    echo "$line"
}

reset()
{
    $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user"
}

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(p) ON $t TO $user"
check "GRANT(p)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $t TO $user"
check "GRANT(p.s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $t TO $user; REVOKE SELECT(\`p.s\`) ON $t FROM $user"
check "GRANT table, REVOKE(p.s)"

$CLICKHOUSE_CLIENT -q "
DROP USER $user;
DROP TABLE t;
"
