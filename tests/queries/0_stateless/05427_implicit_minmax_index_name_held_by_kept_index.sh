#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/126958
# An index named `auto_minmax_index_<column>` that differs from the implicit min-max index of that
# column (another type, or over an ALIAS column) is kept, and no implicit index is generated under
# its name. Atomic databases require an explicit UUID for a full-definition ATTACH.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function list_indices()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT name, type, granularity, creation FROM system.data_skipping_indices
        WHERE database = currentDatabase() AND table = '$1' ORDER BY name"
}

echo "-- load path"
uuid1=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query "
    SET send_logs_level = 'fatal';
    ATTACH TABLE t_held_attach UUID '$uuid1'
    (
        id UInt64,
        a UInt64 ALIAS id * 2,
        b UInt64,
        INDEX auto_minmax_index_a a TYPE minmax GRANULARITY 8,
        INDEX auto_minmax_index_b b TYPE set(100) GRANULARITY 1,
        INDEX auto_minmax_index__block_number id TYPE set(100) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS add_minmax_index_for_numeric_columns = 1, enable_block_number_column = 1, add_minmax_index_for_block_number_column = 1;
"
list_indices t_held_attach
$CLICKHOUSE_CLIENT --query "INSERT INTO t_held_attach (id, b) SELECT number, number FROM numbers(64)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_held_attach WHERE a = 100 SETTINGS force_data_skipping_indices = 'auto_minmax_index_a'"
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_held_attach"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE t_held_attach"
list_indices t_held_attach
$CLICKHOUSE_CLIENT --query "DROP TABLE t_held_attach"

echo "-- alter paths"
uuid2=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --query "
    SET send_logs_level = 'fatal';
    ATTACH TABLE t_held_alter UUID '$uuid2'
    (
        id UInt64,
        x UInt64,
        INDEX auto_minmax_index_b id TYPE minmax GRANULARITY 8,
        INDEX auto_minmax_index_y id TYPE set(100) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS add_minmax_index_for_numeric_columns = 1;
"
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
