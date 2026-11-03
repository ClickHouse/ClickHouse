#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped

# `CREATE HYPOTHETICAL INDEX` and `DROP HYPOTHETICAL INDEX` must not reveal whether a table exists or what its engine
# is to a user who cannot see the table: they are denied before the table is looked up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05491_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE t_mt (secret UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_memory (secret UInt64) ENGINE = Memory;
CREATE USER $user;
"

run()
{
    local out
    if out=$($CLICKHOUSE_CLIENT --user "$user" -q "$1" 2>&1); then
        echo "OK"
    else
        echo "$out" | grep -o -m1 -E 'ACCESS_DENIED|UNKNOWN_TABLE|NOT_IMPLEMENTED' || echo "$out"
    fi
}

for table in t_mt t_memory t_missing; do
    echo "$table"
    run "CREATE HYPOTHETICAL INDEX idx ON ${CLICKHOUSE_DATABASE}.$table (secret) TYPE minmax GRANULARITY 1"
    run "DROP HYPOTHETICAL INDEX IF EXISTS idx ON ${CLICKHOUSE_DATABASE}.$table"
done

# A user who can see the table still gets the usual errors.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_memory TO $user"
echo "t_memory with SELECT"
run "CREATE HYPOTHETICAL INDEX idx ON ${CLICKHOUSE_DATABASE}.t_memory (secret) TYPE minmax GRANULARITY 1"

$CLICKHOUSE_CLIENT -q "
DROP USER $user;
DROP TABLE t_mt;
DROP TABLE t_memory;
"
