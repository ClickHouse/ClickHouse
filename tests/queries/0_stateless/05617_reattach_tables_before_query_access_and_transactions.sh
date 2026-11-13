#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-fasttest
# Tag no-replicated-database: tables of `Replicated` databases are not reattached.
# Tag no-ordinary-database: transactions are not supported in `Ordinary` databases.
# Tag no-fasttest: transactions need Keeper.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `reattach_tables_before_query_execution_probability` runs `DETACH` and `ATTACH` with full access,
# so it must reattach only the tables that the user could reattach itself.
# A `GenerateRandom` table without a seed shows whether it was reattached: it chooses a new seed when it is attached.

user="user_05617_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE gen (x UInt64) ENGINE = GenerateRandom;
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password SETTINGS readonly = 2;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.gen TO $user;
"

function sample()
{
    $CLICKHOUSE_CLIENT --user "$user" -q "SELECT x FROM gen LIMIT 1 SETTINGS max_threads = 1"
}

before=$(sample)
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"
after=$(sample)
[ "$before" = "$after" ] && echo "restricted user: not reattached" || echo "restricted user: reattached"

before=$(sample)
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"
after=$(sample)
[ "$before" = "$after" ] && echo "default user: not reattached" || echo "default user: reattached"

# `ATTACH` also requires `TABLE ENGINE` on the engine of the table (the test configuration has `table_engines_require_grant`).
$CLICKHOUSE_CLIENT -q "
    DROP USER $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, DROP VIEW, CREATE TABLE, CREATE VIEW ON ${CLICKHOUSE_DATABASE}.* TO $user;
"

before=$(sample)
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"
after=$(sample)
[ "$before" = "$after" ] && echo "user without TABLE ENGINE: not reattached" || echo "user without TABLE ENGINE: reattached"

$CLICKHOUSE_CLIENT -q "GRANT TABLE ENGINE ON GenerateRandom TO $user"

before=$(sample)
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"
after=$(sample)
[ "$before" = "$after" ] && echo "user with TABLE ENGINE: not reattached" || echo "user with TABLE ENGINE: reattached"

$CLICKHOUSE_CLIENT -q "DROP USER $user"

# A transaction keeps the tables it has modified until it is finished, so the queries inside it must not reattach tables,
# otherwise `DETACH ... SYNC` would wait for the end of the transaction forever.
# Parallel replicas do not support transactions.

$CLICKHOUSE_CLIENT -q "CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x"

$CLICKHOUSE_CLIENT --reattach_tables_before_query_execution_probability 1 --async_insert 0 --enable_parallel_replicas 0 --max_execution_time 60 -q "
    BEGIN TRANSACTION;
    INSERT INTO mt VALUES (1);
    COMMIT;
    BEGIN TRANSACTION;
    INSERT INTO mt VALUES (2);
    ROLLBACK;
    SELECT 'transactions', groupArray(x) FROM mt;
"

$CLICKHOUSE_CLIENT --reattach_tables_before_query_execution_probability 1 --async_insert 0 --enable_parallel_replicas 0 --implicit_transaction 1 --max_execution_time 60 -q "
    INSERT INTO mt VALUES (3);
    SELECT 'implicit transaction', groupArray(x) FROM (SELECT x FROM mt ORDER BY x);
"
