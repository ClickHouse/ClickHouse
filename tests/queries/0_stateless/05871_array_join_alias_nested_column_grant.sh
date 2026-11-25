#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# a grant on one element of a Nested column is enough for `ARRAY JOIN nst AS n` if the query reads only `n.x`

user="u_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_nested_grant (nst Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_nested_grant VALUES ([1, 2], ['a', 'b']), ([3], ['c']);
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT SELECT(\`nst.x\`) ON t_nested_grant TO ${user};
"

$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT sum(n.x) FROM t_nested_grant ARRAY JOIN nst AS n"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT max(n.y) FROM t_nested_grant ARRAY JOIN nst AS n" 2>&1 | grep -o -m1 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT -q "
    DROP USER ${user};
    DROP TABLE t_nested_grant;
"
