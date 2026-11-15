#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merges of a table that does not set exclude_materialize_statistics_on_merge itself use the value from the
# merge_tree section of the configuration, which is given here on the command line of clickhouse-local.

$CLICKHOUSE_LOCAL --query "
    CREATE TABLE tab (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = 'basic', enable_block_number_column = 0, enable_block_offset_column = 0;

    SYSTEM STOP MERGES tab;
    SET materialize_statistics_on_insert = 0;
    INSERT INTO tab SELECT number, number, number FROM numbers(10);
    INSERT INTO tab SELECT number, number, number FROM numbers(10, 10);
    SYSTEM START MERGES tab;
    OPTIMIZE TABLE tab FINAL;

    SELECT column, statistics != []
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'tab' AND active
    ORDER BY column;
" -- --merge_tree.exclude_materialize_statistics_on_merge=b
