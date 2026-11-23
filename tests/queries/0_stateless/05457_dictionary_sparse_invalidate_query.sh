#!/usr/bin/env bash
# `invalidate_query` must notice changes when a `MergeTree` result column uses `Sparse` serialization.
# Check both the local `ClickHouse` source and the remote path through 127.0.0.2.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
CLIENT="$CLICKHOUSE_CLIENT --ignore_drop_queries_probability=0"
db_name=$($CLIENT -q "SELECT currentDatabase()")

$CLIENT -q "CREATE TABLE dict_sparse_src (id UInt64, val String, upd UInt64) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5"
$CLIENT -q "INSERT INTO dict_sparse_src SELECT number, 'a', number = 1 FROM numbers(1000)"

# Fail rather than silently running the regression against a dense column.
sparse_kind=$($CLIENT -q "SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'dict_sparse_src' AND column = 'upd' AND active")
if [ "$sparse_kind" != "Sparse" ]; then
    echo "Expected Sparse serialization for the invalidation column, got: $sparse_kind" >&2
    exit 1
fi

$CLIENT -q "CREATE DICTIONARY dict_sparse_local (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(USER 'default' DB currentDatabase() TABLE 'dict_sparse_src' INVALIDATE_QUERY 'SELECT upd FROM \`$db_name\`.dict_sparse_src WHERE id = 1')) LAYOUT(FLAT()) LIFETIME(1)"
$CLIENT -q "CREATE DICTIONARY dict_sparse_remote (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST '127.0.0.2' PORT tcpPort() USER 'default' DB currentDatabase() TABLE 'dict_sparse_src' INVALIDATE_QUERY 'SELECT upd FROM \`$db_name\`.dict_sparse_src WHERE id = 1')) LAYOUT(FLAT()) LIFETIME(1)"

wait_until()
{
    local query="$1"
    local deadline=$((SECONDS + 40))
    while [ "$($CLIENT -q "$query")" != "1" ]; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "Timed out waiting for sparse dictionary invalidation" >&2
            return 1
        fi
        sleep 1
    done
}

# Prime both caches, then let the first periodic refresh record the initial
# invalidation value. Otherwise a later reload could pass without comparison.
$CLIENT -q "SELECT dictGet('dict_sparse_local', 'val', toUInt64(1))"
$CLIENT -q "SELECT dictGet('dict_sparse_remote', 'val', toUInt64(1))"
for kind in local remote; do
    dictionary="dict_sparse_$kind"
    last_update=$($CLIENT -q "SELECT toUnixTimestamp(last_successful_update_time) FROM system.dictionaries WHERE database = currentDatabase() AND name = '$dictionary'")
    wait_until "SELECT toUnixTimestamp(last_successful_update_time) > $last_update FROM system.dictionaries WHERE database = currentDatabase() AND name = '$dictionary'"
done

$CLIENT -q "ALTER TABLE dict_sparse_src UPDATE upd = 2, val = 'b' WHERE id = 1 SETTINGS mutations_sync = 1"
for kind in local remote; do
    dictionary="dict_sparse_$kind"
    wait_until "SELECT dictGet('$dictionary', 'val', toUInt64(1)) = 'b'"
    $CLIENT -q "SELECT dictGet('$dictionary', 'val', toUInt64(1))"
done
