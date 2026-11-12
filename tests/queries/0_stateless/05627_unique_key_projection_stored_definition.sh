#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage, no-s3-storage, no-fasttest, no-ordinary-database
# no-shared-merge-tree, no-object-storage, no-s3-storage: UNIQUE KEY needs a plain MergeTree on a disk with transactions.
# no-fasttest: UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.
# no-ordinary-database: UNIQUE KEY tables are not supported in an Ordinary database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A unique-key table that also carries a projection must never read through the projection part (that would
# bypass the delete-bitmap filter). A new definition cannot have the combination, but a stored one still
# loads. The table is stored in a clickhouse-local data directory, its stored definition is rewritten to
# carry a projection, and the next start loads it.

$CLICKHOUSE_CLIENT --enable_unique_key 1 -q "CREATE TABLE uk_t_attach_proj (id UInt64, user_id UInt32, PROJECTION p (SELECT user_id, count() GROUP BY user_id)) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id, user_id); -- { serverError SUPPORT_IS_DISABLED }"

$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/05627_data" --enable_unique_key 1 -q "CREATE TABLE uk_t_attach_proj (id UInt64, user_id UInt32) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id, user_id)"
$CLICKHOUSE_LOCAL --param_metadata="${CLICKHOUSE_TMP:?}/05627_data/metadata/default/uk_t_attach_proj.sql" --param_copy="${CLICKHOUSE_TMP:?}/05627_data.sql" -q "
    SET engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
        SELECT replaceOne(raw_blob, '\`user_id\` UInt32\n)', '\`user_id\` UInt32,\n    PROJECTION p (SELECT user_id, count() GROUP BY user_id)\n)') FROM file({metadata:String}, 'RawBLOB');
    INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"

# Reads of a UNIQUE KEY table need Keeper, which clickhouse-local does not have, so only the load is checked.
$CLICKHOUSE_LOCAL --path="${CLICKHOUSE_TMP:?}/05627_data" --enable_unique_key 1 -q "
    SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 'uk_t_attach_proj';
    DETACH TABLE uk_t_attach_proj;
    ATTACH TABLE uk_t_attach_proj;
    SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 'uk_t_attach_proj';"
rm -rf "${CLICKHOUSE_TMP:?}/05627_data" "${CLICKHOUSE_TMP:?}/05627_data.sql"
