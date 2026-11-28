#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A program of the `q` dialect is compiled before the query is put to the process list, and the compilation analyzes
# the queries that it generates: a table of ten million numbers takes its memory there, even if the program does not use
# the table. That memory is charged to the user, so it is under `max_memory_usage_for_user` when the query has no limit
# of its own. The limit of a user is shared by its queries, so the test has a user of its own.

user="u_05882_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
"

function q()
{
    $CLICKHOUSE_CLIENT --user "${user}" --enable_q_dialect 1 --dialect q --max_memory_usage 0 "$@" 2>&1
}

echo "--- a table that is not used, over the limit of the user"
q --max_memory_usage_for_user 20000000 -q "t:([] a:til 10000000);1" | grep -o -m1 'MEMORY_LIMIT_EXCEEDED'

echo "--- a small table, under the limit of the user"
q --max_memory_usage_for_user 20000000 -q "t:([] a:til 3);1"

echo "--- no limit of the user"
q --max_memory_usage_for_user 0 -q "t:([] a:til 10000000);1"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
