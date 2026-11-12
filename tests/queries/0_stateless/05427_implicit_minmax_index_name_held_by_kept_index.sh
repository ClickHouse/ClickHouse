#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/126958
# An index named `auto_minmax_index_<column>` that differs from the implicit min-max index of that
# column (another type, or over an ALIAS column) is kept, and no implicit index is generated under
# its name. A new definition cannot use such a name, so the tables are stored in a clickhouse-local data
# directory with other index names, their stored definitions are rewritten to the reserved names, and
# the next start loads them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_dir="${CLICKHOUSE_TMP}/data"
function local_query()
{
    $CLICKHOUSE_LOCAL --path="$data_dir" "$@"
}

function list_indices()
{
    local_query --query "
        SELECT name, type, granularity, creation FROM system.data_skipping_indices
        WHERE database = currentDatabase() AND table = '$1' ORDER BY name"
}

# Creates table $1 with the definition $2, whose index names start with `held_`, and gives those
# indices the reserved `auto_minmax_index_` prefix in the stored definition.
function create_stored_with_reserved_names()
{
    local_query --query "CREATE TABLE $1 $2"
    $CLICKHOUSE_LOCAL --param_metadata="$data_dir/metadata/default/$1.sql" --param_copy="$data_dir.sql" --query "
        SET engine_file_truncate_on_insert = 1;
        INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
            SELECT replaceAll(raw_blob, 'INDEX held_', 'INDEX auto_minmax_index_') FROM file({metadata:String}, 'RawBLOB');
        INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
}

echo "-- a new definition cannot use a reserved name"
# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
UUID_CLAUSE=""
if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
fi
$CLICKHOUSE_CLIENT --query "
    ATTACH TABLE t_held_attach ${UUID_CLAUSE} (id UInt64, b UInt64, INDEX auto_minmax_index_b b TYPE set(100) GRANULARITY 1)
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
local_query --query "INSERT INTO t_held_attach (id, b) SELECT number, number FROM numbers(64)"
local_query --query "SELECT count() FROM t_held_attach WHERE a = 100 SETTINGS force_data_skipping_indices = 'auto_minmax_index_a'"
local_query --query "DETACH TABLE t_held_attach; ATTACH TABLE t_held_attach"
list_indices t_held_attach

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
local_query --query "INSERT INTO t_held_alter SELECT number, number FROM numbers(16)"
local_query --query "ALTER TABLE t_held_alter ADD COLUMN b UInt64"
local_query --mutations_sync 2 --query "ALTER TABLE t_held_alter RENAME COLUMN x TO y"
echo "-- after alter"
list_indices t_held_alter
local_query --query "SELECT count() FROM t_held_alter WHERE y = 3"
local_query --query "CHECK TABLE t_held_alter SETTINGS check_query_single_value_result = 1"
local_query --query "DETACH TABLE t_held_alter; ATTACH TABLE t_held_alter"
echo "-- after reload"
list_indices t_held_alter
rm -rf "$data_dir" "$data_dir.sql"
