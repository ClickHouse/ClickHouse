#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ON CLUSTER` hosts run the query without the submitting user and expand an `ALIAS` column in a
# text index transform against their own schema, so the initiator sends the expansion it authorized.

USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n -q "
    DROP USER IF EXISTS $USER;
    CREATE TABLE tab (id UInt64, val String, lowered String ALIAS lower(val)) ENGINE = MergeTree ORDER BY id;
    SET distributed_ddl_output_mode = 'none';
    ALTER TABLE tab ON CLUSTER test_shard_localhost
        ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lowered);
"

echo '1. The dispatched transform is the expansion of the alias.'
$CLICKHOUSE_CLIENT -q "SELECT type_full FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab'"

# With an explicit engine `setEngine` does not look at `AS src`, and the inherited indices are inspected
# on the initiator, so describing the source has to be authorized first.
$CLICKHOUSE_CLIENT -n -q "
    CREATE USER $USER;
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.dst TO $USER;
    GRANT TABLE ENGINE ON MergeTree TO $USER;
"

echo '2. CREATE AS with an explicit engine and no privilege on the source.'
$CLICKHOUSE_CLIENT --user "$USER" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode none -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.tab ENGINE = MergeTree ORDER BY tuple()
" 2>&1 | grep -o -m1 "SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.tab"

echo '3. The same for a source that does not exist.'
$CLICKHOUSE_CLIENT --user "$USER" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode none -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.dst ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.missing ENGINE = MergeTree ORDER BY tuple()
" 2>&1 | grep -o -m1 "SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.missing"

$CLICKHOUSE_CLIENT -n -q "
    DROP USER $USER;
    DROP TABLE tab;
"
