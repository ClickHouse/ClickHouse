#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A text bloom filter index with the total size of filters over the limit cannot be created anymore,
# but a table created before the check still loads and can be altered, as long as its index does not grow.

# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
UUID_CLAUSE=""
if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
fi

# A full-definition ATTACH is validated like CREATE.
$CLICKHOUSE_CLIENT -q "
ATTACH TABLE t_bf_total_size_attach ${UUID_CLAUSE} (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
"

# The table is stored in a clickhouse-local data directory, its stored definition is rewritten to carry
# the oversized index, and the next start loads it.
$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/05422_data" -q "CREATE TABLE t_bf_total_size_attach (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(1024, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_LOCAL --param_metadata="${CLICKHOUSE_TMP:?}/05422_data/metadata/default/t_bf_total_size_attach.sql" --param_copy="${CLICKHOUSE_TMP:?}/05422_data.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, 'tokenbf_v1(1024, 3, 0)', 'tokenbf_v1(536870913, 3, 0)') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"

$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/05422_data" -m -q "
SELECT type_full FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_bf_total_size_attach';
ALTER TABLE t_bf_total_size_attach ADD COLUMN c String;
ALTER TABLE t_bf_total_size_attach RENAME COLUMN b TO d;
ALTER TABLE t_bf_total_size_attach ADD INDEX idx2 (a, c) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_bf_total_size_attach' ORDER BY position;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_bf_total_size_attach';
"
rm -rf "${CLICKHOUSE_TMP:?}/05422_data" "${CLICKHOUSE_TMP:?}/05422_data.sql"
