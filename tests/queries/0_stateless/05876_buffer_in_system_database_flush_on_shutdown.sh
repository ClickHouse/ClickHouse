#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Buffer` table in the `system` or `information_schema` database is flushed on shutdown, before its
# destination in a user database shuts down.
# Note: $CLICKHOUSE_LOCAL is not used here because it passes --tmp.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"
mkdir -p "$data_dir"

local_path()
{
    ${CLICKHOUSE_BINARY} local --path "$data_dir" "$@"
}

local_path --query "
    CREATE DATABASE dst;
    CREATE TABLE dst.mt (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE system.buf (x UInt64) ENGINE = Buffer(dst, mt, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
    CREATE TABLE information_schema.buf (x UInt64) ENGINE = Buffer(dst, mt, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
    INSERT INTO system.buf VALUES (1);
    INSERT INTO information_schema.buf VALUES (2);
"

local_path --query "SELECT * FROM dst.mt ORDER BY x"

rm -rf "$data_dir"
