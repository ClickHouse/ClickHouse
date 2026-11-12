#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage, no-s3-storage
# no-shared-merge-tree, no-object-storage, no-s3-storage: UNIQUE KEY needs a plain MergeTree on a disk with transactions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stored UNIQUE KEY + TTL table (which a new definition cannot have) still loads. The table is stored
# in a clickhouse-local data directory without TTL, its stored definition is rewritten to carry one, and
# the next start loads it. MATERIALIZE TTL on it is rejected, REMOVE TTL is allowed.

data_dir="${CLICKHOUSE_TMP}/data"
$CLICKHOUSE_LOCAL --path="$data_dir" --enable_unique_key 1 -q "CREATE TABLE uk_ttl_attach (id UInt64, d Date, v String) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)"
$CLICKHOUSE_LOCAL --param_metadata="$data_dir/metadata/default/uk_ttl_attach.sql" --param_copy="$data_dir.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, '\nUNIQUE KEY (id)\n', '\nUNIQUE KEY (id) TTL d + toIntervalDay(1)\n') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
$CLICKHOUSE_LOCAL --path="$data_dir" -q "
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'uk_ttl_attach' AND create_table_query LIKE '%TTL%';
    ALTER TABLE uk_ttl_attach MATERIALIZE TTL; -- { serverError SUPPORT_IS_DISABLED }
    ALTER TABLE uk_ttl_attach REMOVE TTL"
rm -rf "$data_dir" "$data_dir.sql"
