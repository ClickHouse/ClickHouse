#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table stored by an affected version may already have a default over a virtual column. Loading it
# and an unrelated ALTER on it must still work; the stricter check applies only to new and modified
# columns. The table is stored in a clickhouse-local data directory, its stored definition is rewritten
# to carry such a default, and the next start loads it.

data_dir="${CLICKHOUSE_TMP}/data"
$CLICKHOUSE_LOCAL --path="$data_dir" -q "CREATE TABLE legacy (c0 String MATERIALIZED 'x', c1 UInt16) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_LOCAL --param_metadata="$data_dir/metadata/default/legacy.sql" --param_copy="$data_dir.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, 'MATERIALIZED \'x\'', 'MATERIALIZED _table') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
$CLICKHOUSE_LOCAL --path="$data_dir" -q "
    SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 'legacy' AND name = 'c0';
    ALTER TABLE legacy ADD COLUMN c2 UInt8 DEFAULT 0;
    SELECT 'legacy attach and unrelated alter ok'"
rm -rf "$data_dir" "$data_dir.sql"
