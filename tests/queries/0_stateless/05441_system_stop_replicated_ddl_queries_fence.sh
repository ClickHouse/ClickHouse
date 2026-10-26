#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database
# no-parallel: SYSTEM STOP REPLICATED DDL QUERIES affects all Replicated databases on the server
# no-replicated-database: the test creates its own Replicated databases

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_DEFAULT="${CLICKHOUSE_DATABASE}_default_keeper"
DB_AUX="${CLICKHOUSE_DATABASE}_aux_keeper"
USER_NONE="u_none_${CLICKHOUSE_DATABASE}"
USER_TABLE="u_table_${CLICKHOUSE_DATABASE}"
USER_GLOBAL="u_global_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m --query "
    DROP DATABASE IF EXISTS $DB_DEFAULT SYNC;
    DROP DATABASE IF EXISTS $DB_AUX SYNC;
    -- Explicit engine arguments: the commands must not depend on the '{replica}' macro.
    CREATE DATABASE $DB_DEFAULT ENGINE = Replicated('/test/databases/$CLICKHOUSE_DATABASE/fence_default', 's1', 'r1');
    CREATE DATABASE $DB_AUX ENGINE = Replicated('zookeeper2:/test/databases/$CLICKHOUSE_DATABASE/fence_aux', 's1', 'r1');
    DROP USER IF EXISTS $USER_NONE, $USER_TABLE, $USER_GLOBAL;
    CREATE USER $USER_NONE, $USER_TABLE, $USER_GLOBAL;
    GRANT SYSTEM REPLICATION QUEUES ON $DB_DEFAULT.* TO $USER_TABLE;
    GRANT SYSTEM REPLICATION QUEUES ON *.* TO $USER_GLOBAL;
"

# Prints whether the stop flag of this server (named '<server UUID>_<process ID>_<Keeper name>') exists in the default and in the auxiliary Keeper, and whether it is ephemeral.
print_flags() {
    for keeper in default zookeeper2; do
        $CLICKHOUSE_CLIENT --query "
            SELECT '$keeper', count(), sum(ephemeralOwner != 0)
            FROM system.zookeeper
            WHERE path = '/clickhouse/stop_replicated_ddl_queries' AND startsWith(name, toString(serverUUID()) || '_') AND endsWith(name, '_$keeper') AND zookeeperName = '$keeper'
            HAVING count() > 0"
    done
    echo "--"
}

try_ddl() {
    local out
    out=$($CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none --query "CREATE TABLE $1.$2 (x UInt64) ENGINE = MergeTree ORDER BY x" 2>&1)
    if grep -qF QUERY_IS_PROHIBITED <<< "$out"; then
        echo "QUERY_IS_PROHIBITED"
    elif [ -z "$out" ]; then
        echo "ok"
    else
        echo "$out"
    fi
}

echo "denied"
for user in $USER_NONE $USER_TABLE; do
    $CLICKHOUSE_CLIENT --user "$user" --query "SYSTEM STOP REPLICATED DDL QUERIES" 2>&1 | grep -o -m1 ACCESS_DENIED
    $CLICKHOUSE_CLIENT --user "$user" --query "SYSTEM START REPLICATED DDL QUERIES" 2>&1 | grep -o -m1 ACCESS_DENIED
done
# Denied attempts do not change the state.
print_flags
try_ddl "$DB_DEFAULT" t0

echo "stop"
$CLICKHOUSE_CLIENT --user "$USER_GLOBAL" --query "SYSTEM STOP REPLICATED DDL QUERIES"
print_flags
try_ddl "$DB_DEFAULT" t1
try_ddl "$DB_AUX" t1

# A denied START leaves DDL queries stopped.
$CLICKHOUSE_CLIENT --user "$USER_TABLE" --query "SYSTEM START REPLICATED DDL QUERIES" 2>&1 | grep -o -m1 ACCESS_DENIED
print_flags
try_ddl "$DB_DEFAULT" t1

echo "start"
$CLICKHOUSE_CLIENT --user "$USER_GLOBAL" --query "SYSTEM START REPLICATED DDL QUERIES"
print_flags
try_ddl "$DB_DEFAULT" t1
try_ddl "$DB_AUX" t1

# START removes the flag from the Keeper of a database that was detached after STOP.
echo "detach"
$CLICKHOUSE_CLIENT --user "$USER_GLOBAL" --query "SYSTEM STOP REPLICATED DDL QUERIES"
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB_AUX"
$CLICKHOUSE_CLIENT --user "$USER_GLOBAL" --query "SYSTEM START REPLICATED DDL QUERIES"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE $DB_AUX"
print_flags
try_ddl "$DB_AUX" t2

$CLICKHOUSE_CLIENT --query "SELECT replaceOne(database, '$CLICKHOUSE_DATABASE', ''), name FROM system.tables WHERE database IN ('$DB_DEFAULT', '$DB_AUX') ORDER BY ALL"

$CLICKHOUSE_CLIENT -m --query "
    DROP DATABASE $DB_DEFAULT SYNC;
    DROP DATABASE $DB_AUX SYNC;
    DROP USER $USER_NONE, $USER_TABLE, $USER_GLOBAL;
"
