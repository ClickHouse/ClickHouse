#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Parquet` format of data parts is experimental: under `allow_feature_tier = 1` it cannot be enabled
# by `CREATE`, `ALTER`, or the settings of a projection.

function restricted()
{
    echo -n "$1: "
    ${CLICKHOUSE_LOCAL} -m -q "CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k; $1" -- --allow_feature_tier=1 2>&1 \
        | grep -q -F "Changes to EXPERIMENTAL settings are disabled" && echo "refused" || echo "ok"
}

restricted "CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_rows_for_parquet_part = 1"
restricted "CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_parquet_part = 1"
restricted "CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_level_for_parquet_part = 1"
restricted "ALTER TABLE t MODIFY SETTING min_rows_for_parquet_part = 1"
restricted "ALTER TABLE t ADD PROJECTION p (SELECT k ORDER BY k) WITH SETTINGS (min_rows_for_parquet_part = 1)"
restricted "CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS parquet_row_group_granularity_rows = 1000"
