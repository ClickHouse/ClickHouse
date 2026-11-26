#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client adds the MergeTree settings from its command line to `CREATE TABLE` only if the query does not set them,
# including by an alias. Otherwise the added canonical name would override the value set by the alias.

$CLICKHOUSE_CLIENT --allow_merge_tree_settings \
    --object_shared_data_serialization_version_for_small_parts map \
    --min_bytes_for_object_shared_data_main_serialization_version 12345 \
    -q "CREATE TABLE test (json JSON) ENGINE = MergeTree ORDER BY tuple() SETTINGS object_shared_data_serialization_version_for_zero_level_parts = 'advanced'"

$CLICKHOUSE_CLIENT -q "
    SELECT
        position(create_table_query, 'object_shared_data_serialization_version_for_zero_level_parts = \'advanced\'') > 0,
        NOT match(create_table_query, '[^0-9A-Za-z_]object_shared_data_serialization_version_for_small_parts ='),
        position(create_table_query, 'min_bytes_for_object_shared_data_main_serialization_version = 12345') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'test'"

$CLICKHOUSE_CLIENT -q "DROP TABLE test"
