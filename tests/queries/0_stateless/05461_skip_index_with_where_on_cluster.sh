#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: ON CLUSTER is not allowed for a Replicated database

# Hosts of an `ON CLUSTER` query analyse a partial index condition without the user, against their own schema.
# Only a declared column list is the same everywhere, so only it is checked on the initiator; the rest is refused.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05461_cluster_${CLICKHOUSE_DATABASE}"
CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_partial_skip_indexes 1"

function run()
{
    local output
    output=$($CLIENT "$@" 2>&1)
    if [[ "$output" == *distributed_ddl_use_initial_user_and_roles* ]]; then
        echo "refused"
    else
        { echo "$output" | grep -o -E '\([A-Z_]{4,}\)' || echo "ok"; } | head -1
    fi
}

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE secret (sensitive UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tab (p UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE USER $user;
GRANT CREATE TABLE, ALTER ADD INDEX, CLUSTER ON *.* TO $user;
"

echo '1. A condition the user may run is accepted with a declared column list, refused on an existing table.'
run -q "ALTER TABLE tab ON CLUSTER test_shard_localhost ADD INDEX i p TYPE minmax WHERE p > 0"
run -q "CREATE INDEX j ON tab ON CLUSTER test_shard_localhost (p) TYPE minmax WHERE p > 1"
run -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost
    (p UInt8, INDEX i p TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()
"
$CLICKHOUSE_CLIENT -q "SELECT table, name FROM system.data_skipping_indices WHERE database = currentDatabase() ORDER BY ALL"

echo '2. An index copied by CREATE AS is refused, on every entry format.'
for version in 2 8
do
    run --distributed_ddl_entry_format_version $version -q "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.copy_$version ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.dst
    "
done
$CLICKHOUSE_CLIENT -q "SELECT table, name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table LIKE 'copy%' ORDER BY ALL"

echo '3. A condition the user may not run is refused.'
predicate="p = intDiv(1, hasColumnInTable('$CLICKHOUSE_DATABASE', 'secret', 'sensitive'))"
run --user "$user" -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.tab ON CLUSTER test_shard_localhost ADD INDEX k p TYPE minmax WHERE $predicate"
run --user "$user" -q "CREATE INDEX k ON ${CLICKHOUSE_DATABASE}.tab ON CLUSTER test_shard_localhost (p) TYPE minmax WHERE $predicate"
run --user "$user" -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.probe ON CLUSTER test_shard_localhost
    (p UInt8, INDEX i p TYPE minmax WHERE $predicate) ENGINE = MergeTree ORDER BY tuple()
"

echo '4. A table this host does not have is refused.'
run -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.absent ON CLUSTER test_shard_localhost ADD INDEX i p TYPE minmax WHERE p > 0"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'probe'"
$CLICKHOUSE_CLIENT -q "DROP USER $user"
