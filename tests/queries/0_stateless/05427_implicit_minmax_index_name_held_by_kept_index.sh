#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/126958
# An index named `auto_minmax_index_<column>` that differs from the implicit min-max index of that
# column (another type, or over an ALIAS column) is kept, and no implicit index is generated under
# its name. A new definition cannot use such a name, so the tables are stored with other index names,
# their stored definitions are rewritten to the reserved names, and they are attached back with the short ATTACH.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function list_indices()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT name, type, granularity, creation FROM system.data_skipping_indices
        WHERE database = currentDatabase() AND table = '$1' ORDER BY name"
}

# Creates table $1 with the definition $2, whose index names start with `held_`, and gives those
# indices the reserved `auto_minmax_index_` prefix in the stored definition.
function create_stored_with_reserved_names()
{
    $CLICKHOUSE_CLIENT --query "CREATE TABLE $1 $2"
    local metadata_file
    metadata_file=$($CLICKHOUSE_CLIENT --query "
        SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = '$1'")
    $CLICKHOUSE_CLIENT --query "DETACH TABLE $1"
    sed -i.bak 's/INDEX held_/INDEX auto_minmax_index_/' "$metadata_file" && rm -f "$metadata_file.bak"
    $CLICKHOUSE_CLIENT --query "ATTACH TABLE $1"
}

echo "-- a new definition cannot use a reserved name"
uuid1=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query "
    ATTACH TABLE t_held_attach UUID '$uuid1' (id UInt64, b UInt64, INDEX auto_minmax_index_b b TYPE set(100) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS add_minmax_index_for_numeric_columns = 1; -- { serverError BAD_ARGUMENTS }
"

echo "-- load path"
create_stored_with_reserved_names t_held_attach "
    (
        id UInt64,
        a UInt64 ALIAS id * 2,
        b UInt64,
        INDEX held_a a TYPE minmax GRANULARITY 8,
        INDEX held_b b TYPE set(100) GRANULARITY 1,
        INDEX held__block_number id TYPE set(100) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS add_minmax_index_for_numeric_columns = 1, enable_block_number_column = 1, add_minmax_index_for_block_number_column = 1"
list_indices t_held_attach
$CLICKHOUSE_CLIENT --query "INSERT INTO t_held_attach (id, b) SELECT number, number FROM numbers(64)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_held_attach WHERE a = 100 SETTINGS force_data_skipping_indices = 'auto_minmax_index_a'"
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_held_attach"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE t_held_attach"
list_indices t_held_attach
$CLICKHOUSE_CLIENT --query "DROP TABLE t_held_attach"

echo "-- alter paths"
create_stored_with_reserved_names t_held_alter "
    (
        id UInt64,
        x UInt64,
        INDEX held_b id TYPE minmax GRANULARITY 8,
        INDEX held_y id TYPE set(100) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS add_minmax_index_for_numeric_columns = 1"
list_indices t_held_alter
$CLICKHOUSE_CLIENT --query "INSERT INTO t_held_alter SELECT number, number FROM numbers(16)"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_held_alter ADD COLUMN b UInt64"
$CLICKHOUSE_CLIENT --mutations_sync 2 --query "ALTER TABLE t_held_alter RENAME COLUMN x TO y"
echo "-- after alter"
list_indices t_held_alter
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_held_alter WHERE y = 3"
$CLICKHOUSE_CLIENT --query "CHECK TABLE t_held_alter SETTINGS check_query_single_value_result = 1"
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_held_alter"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE t_held_alter"
echo "-- after reload"
list_indices t_held_alter
$CLICKHOUSE_CLIENT --query "DROP TABLE t_held_alter"
