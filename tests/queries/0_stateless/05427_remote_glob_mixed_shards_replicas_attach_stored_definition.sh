#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `table_function_remote_max_addresses` limits the total number of addresses over the shards and their
# replicas, while older versions limited each of the two expansion stages separately. A definition the
# server stored itself has to load under the old rules, otherwise upgrading the server would make the
# table (or the whole server) fail to load. `127.0.0.{1,2}:{9000|9001}` is two shards with two replicas
# each: four addresses in total, but at most two at each stage.
ADDRESSES="127.0.0.{1,2}:{9000|9001}"

# A new definition is checked against the total.
$CLICKHOUSE_CLIENT --table_function_remote_max_addresses 3 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.remote_mixed_glob (dummy UInt8) ENGINE = Remote('$ADDRESSES', system, one)" 2>&1 \
    | grep -oF -e "Table engine 'Remote'" -e "too many result addresses: 4, while at most 3 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --table_function_remote_max_addresses 4 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.remote_mixed_glob (dummy UInt8) ENGINE = Remote('$ADDRESSES', system, one)"
$CLICKHOUSE_CLIENT --query "DETACH TABLE ${CLICKHOUSE_DATABASE}.remote_mixed_glob"

# The stored definition is replayed by the short `ATTACH` and loads although the total exceeds the limit.
$CLICKHOUSE_CLIENT --table_function_remote_max_addresses 3 --query "ATTACH TABLE ${CLICKHOUSE_DATABASE}.remote_mixed_glob"
$CLICKHOUSE_CLIENT --query "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'remote_mixed_glob'"
$CLICKHOUSE_CLIENT --query "DROP TABLE ${CLICKHOUSE_DATABASE}.remote_mixed_glob"
