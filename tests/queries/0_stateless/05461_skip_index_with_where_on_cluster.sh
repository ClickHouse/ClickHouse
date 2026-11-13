#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: ON CLUSTER is not allowed for a Replicated database

# The hosts of an `ON CLUSTER` query analyse the condition of a partial index without the submitting user,
# so it is refused unless they run as that user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_partial_skip_indexes 1 --distributed_ddl_output_mode none"

$CLIENT -q "CREATE TABLE tab (p UInt8) ENGINE = MergeTree ORDER BY tuple()"

echo '1. ALTER ADD INDEX with WHERE is refused.'
$CLIENT -q "ALTER TABLE tab ON CLUSTER test_shard_localhost ADD INDEX i p TYPE minmax WHERE p > 0" 2>&1 \
    | grep -c -m1 "distributed_ddl_use_initial_user_and_roles"

echo '2. CREATE INDEX with WHERE is refused.'
$CLIENT -q "CREATE INDEX i ON tab ON CLUSTER test_shard_localhost (p) TYPE minmax WHERE p > 0" 2>&1 \
    | grep -c -m1 "distributed_ddl_use_initial_user_and_roles"

echo '3. CREATE TABLE with such an index is refused.'
$CLIENT -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost
    (p UInt8, INDEX i p TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()
" 2>&1 | grep -c -m1 "distributed_ddl_use_initial_user_and_roles"

echo '4. An index copied by CREATE AS is refused, on the legacy entry format too.'
$CLIENT -q "CREATE TABLE src (p UInt8, INDEX i p TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()"
for version in 2 8
do
    $CLIENT --distributed_ddl_entry_format_version $version -q "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.src
    " 2>&1 | grep -c -m1 "distributed_ddl_use_initial_user_and_roles"
done

echo '5. An index without WHERE is unaffected.'
$CLIENT -q "ALTER TABLE tab ON CLUSTER test_shard_localhost ADD INDEX plain p TYPE minmax"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab'"

$CLICKHOUSE_CLIENT -q "DROP TABLE tab; DROP TABLE src; DROP TABLE IF EXISTS dst"
