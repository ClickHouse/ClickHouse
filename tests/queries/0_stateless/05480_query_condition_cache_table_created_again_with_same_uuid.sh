#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-ordinary-database, no-replicated-database
# Tag no-fasttest: needs Keeper for the Replicated database
# Tag no-ordinary-database: the table needs a UUID
# Tag no-replicated-database: an explicit UUID is not allowed in a Replicated database, and the test creates its own one

# A table created again with the same UUID names its parts like the previous table with this UUID did,
# and must not reuse the cached data of those parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function create()
{
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS tab SYNC;
        CREATE TABLE tab UUID '$1' (a UInt64) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';"
}

# use_statistics_for_part_pruning = 0: the statistics would skip the part with no match before the cache records it.
function query()
{
    for prewhere in 0 1; do
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM $1 WHERE a < 50
            SETTINGS use_query_condition_cache = 1, use_statistics_for_part_pruning = 0, use_uncompressed_cache = 1,
                use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
                optimize_move_to_prewhere = $prewhere"
    done
}

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number + 100 FROM numbers(100)"
query tab
# The same number of granules, half of the rows match.
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number FROM numbers(100)"
query tab

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number + 100 FROM numbers(100)"
query tab
# More granules than the dropped part had.
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number FROM numbers(1000)"
query tab

# ATTACH with a full definition creates a new table too.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number + 100 FROM numbers(100)"
query tab
# send_logs_level: the server warns that ATTACH with a full definition is not recommended.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    DROP TABLE tab SYNC;
    ATTACH TABLE tab UUID '$uuid' (a UInt64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number FROM numbers(100)"
query tab

# A table moved to a database without UUIDs keeps its parts, and its UUID can be used again.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number + 100 FROM numbers(100)"
query tab
# send_logs_level: the server warns once about the first Ordinary database.
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 --send_logs_level=error -q "
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_ordinary SYNC;
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_ordinary ENGINE = Ordinary;
    RENAME TABLE tab TO ${CLICKHOUSE_DATABASE}_ordinary.tab;"
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number FROM numbers(100)"
query tab

$CLICKHOUSE_CLIENT -q "DROP TABLE tab SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_ordinary SYNC"

# Replica recovery of a Replicated database creates a table again with the UUID stored in Keeper.
DB="${CLICKHOUSE_DATABASE}_rdb"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb"
# The queries that rewrite Keeper nodes must run as written (the stress profile fuzzes every query).
ZK_WRITER="${CLICKHOUSE_CLIENT} --ast_fuzzer_runs=0 --ast_fuzzer_any_query=0"

# Recovery rewrites the replica digest as its last step.
function recovered()
{
    $CLICKHOUSE_CLIENT -q "SELECT value != '42' FROM system.zookeeper WHERE path = '${ZK_PATH}/replicas/s1|r1' AND name = 'digest'"
}

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB} SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}_broken_tables SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${DB}.tab (a UInt64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi'"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.tab SELECT number + 100 FROM numbers(100)"
query "${DB}.tab"

# Keeper has another definition, so recovery moves the local table away and creates it again with its UUID.
${ZK_WRITER} -q "
    INSERT INTO system.zookeeper (path, name, value)
    SELECT path, name, concat(trimRight(value), '\nCOMMENT \'altered\'\n') FROM system.zookeeper
    WHERE path = '${ZK_PATH}/metadata' AND name = 'tab'"
${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/replicas/s1|r1/digest' '42'"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB}"
for _ in {1..240}; do
    [ "$(recovered)" = "1" ] && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "SELECT 'recovered', $(recovered)"

$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.tab SELECT number FROM numbers(100)"
query "${DB}.tab"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB} SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}_broken_tables SYNC"
