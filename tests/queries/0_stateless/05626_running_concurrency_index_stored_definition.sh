#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A secondary index over a non-deterministic function is refused in a new definition, but a table an
# earlier version stored with such an index still loads.

$CLICKHOUSE_CLIENT -q "CREATE TABLE legacy_index (a UInt64, s DateTime, e DateTime, INDEX i runningConcurrency(s, e) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a; -- { serverError BAD_ARGUMENTS }"

# The stored table is emulated in a clickhouse-local data directory: a table with a deterministic index
# is created, its stored definition is rewritten, and the next start loads it.
data_dir="${CLICKHOUSE_TMP}/data"
$CLICKHOUSE_LOCAL --path="$data_dir" -q "CREATE TABLE legacy_index (a UInt64, s DateTime, e DateTime, INDEX i s TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_LOCAL --param_metadata="$data_dir/metadata/default/legacy_index.sql" --param_copy="$data_dir.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, 'INDEX i s TYPE minmax', 'INDEX i runningConcurrency(s, e) TYPE minmax') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
$CLICKHOUSE_LOCAL --path="$data_dir" -q "
    SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'legacy_index';
    SELECT 'legacy index attaches', count() FROM legacy_index"
rm -rf "$data_dir" "$data_dir.sql"
