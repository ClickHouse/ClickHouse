#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-shared-merge-tree, no-fasttest, no-msan, zookeeper
# no-parallel: `/replicas_status` loads every deferred replicated table on the server, which breaks the other tests
# that check that their replicated tables are still stand-ins.
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
# no-shared-merge-tree: the table engine is replaced.
# no-fasttest: delta-kernel-rs is not in the fast test.
# no-msan: delta-kernel-rs is not built with MSan.

# With `lazy_load_tables` the catalog holds a stand-in for a table until something uses it. A replica that was
# never loaded cannot report its delay, so the `/replicas_status` handler has to load it. `supportsReplication`
# is also true for the data lake engines, which have no delay, so the handler must not load those: a routine
# health probe would otherwise materialize every one of them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
LAKE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_lake"
rm -rf "$LAKE_PATH"

function reload()
{
    # A background task, such as the end of a mutation, can hold a table for a moment longer, and the database
    # refuses to be detached until it lets go.
    local error
    for _ in {1..600}; do
        error=$($CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\`" 2>&1) && break
        if [[ $error != *DATABASE_NOT_EMPTY* ]]; then
            echo "$error" >&2
            break
        fi
        sleep 0.1
    done
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
}

function loaded()
{
    $CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"
}

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.replica (id UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/replica', 'r1') ORDER BY id;
    INSERT INTO \`$DB\`.replica SELECT number FROM numbers(10);
    SET allow_experimental_delta_kernel_rs = 1, allow_delta_lake_writes = 1, allow_delta_lake_create_table = 1;
    CREATE TABLE \`$DB\`.lake (id Int32) ENGINE = DeltaLakeLocal('$LAKE_PATH', Parquet);"
reload
echo "- stand-ins"
loaded

# The other tests on the server decide whether the answer is `Ok`, so only its effect is checked here.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/replicas_status" > /dev/null
echo "- after /replicas_status"
loaded
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.replicas WHERE database = '$DB' AND table = 'replica'"

# The replica-divergence check of a nondeterministic mutation must see the replicated engine.
reload
$CLICKHOUSE_CLIENT -q "ALTER TABLE \`$DB\`.replica UPDATE id = rand() WHERE 1" 2>&1 | grep -o -m1 BAD_ARGUMENTS

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
rm -rf "$LAKE_PATH"
