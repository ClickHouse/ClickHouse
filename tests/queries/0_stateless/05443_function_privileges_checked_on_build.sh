#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: introspection functions need the symbols of the binary
# no-replicated-database: creates users

# Functions that require privileges check them when they are built with their arguments, not when they are
# looked up in `FunctionFactory`. A query that uses such a function still fails at analysis, and a code path
# that only looks a function up (here: on-the-fly mutations check if the functions of a pending mutation are
# deterministic) does not need the privilege.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05443_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};

    CREATE TABLE t (id UInt64, x String, y String) ENGINE = MergeTree ORDER BY id;
    SYSTEM STOP MERGES t;
    INSERT INTO t VALUES (1, 'a', 'b');
    ALTER TABLE t UPDATE x = demangle('_Z3foov') WHERE 1 SETTINGS allow_introspection_functions = 1, mutations_sync = 0;
"

# Print the message without the name of the user and without the context added by the analyzer, and the error code.
function error()
{
    local out
    out=$("$@" 2>&1 | grep -m1 -E "Not enough privileges|Introspection functions are disabled")
    echo "$(echo "$out" | grep -o -E "(Not enough privileges|Introspection functions are disabled)[^:]*" | sed -E 's/\. \([A-Z_]+\).*//')" \
        "$(echo "$out" | grep -o -m1 -E "\((ACCESS_DENIED|FUNCTION_NOT_ALLOWED)\)")"
}

function user_query()
{
    error ${CLICKHOUSE_CLIENT} --user "${user}" -q "$1"
}

echo "--- queries of a user without the privileges"
user_query "SELECT demangle('_Z3foov')"
user_query "SELECT demangle(NULL)"
user_query "EXPLAIN SELECT demangle('_Z3foov')"
user_query "SELECT if(id > 100, demangle(x), '') FROM t WHERE 0"
user_query "SELECT demangle(x) FROM t SETTINGS apply_mutations_on_fly = 1"
user_query "SELECT file('a.txt')"
user_query "SELECT getMacro('replica')"
user_query "SELECT getServerSetting('max_thread_pool_size')"
user_query "SELECT getMergeTreeSetting('index_granularity')"

echo "--- introspection functions are disabled"
error ${CLICKHOUSE_CLIENT} --allow_introspection_functions 0 -q "SELECT demangle('_Z3foov')"
error ${CLICKHOUSE_CLIENT} --allow_introspection_functions 0 -q "SELECT addressToSymbol(0)"
error ${CLICKHOUSE_CLIENT} --allow_introspection_functions 0 -q "SELECT addressToLine(0)"
error ${CLICKHOUSE_CLIENT} --allow_introspection_functions 0 -q "SELECT addressToLineWithInlines(0)"
error ${CLICKHOUSE_CLIENT} --allow_introspection_functions 0 -q "CREATE TABLE t_default (s String DEFAULT demangle('_Z3foov')) ENGINE = MergeTree ORDER BY ()"

echo "--- the pending mutation uses demangle, the user reads only the other columns"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT id, y FROM t ORDER BY id SETTINGS apply_mutations_on_fly = 1"
echo "--- the pending mutation is applied when the user reads the updated column"
user_query "SELECT x FROM t ORDER BY id SETTINGS apply_mutations_on_fly = 1"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM t ORDER BY id SETTINGS apply_mutations_on_fly = 1, allow_introspection_functions = 1"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
