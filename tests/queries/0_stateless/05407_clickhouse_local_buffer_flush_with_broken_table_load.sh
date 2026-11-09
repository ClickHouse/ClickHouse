#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Buffer` table in `clickhouse-local` is flushed to its destination on shutdown, even if an unrelated
# persisted table failed to load. Such a table must not hide the other tables of its database from the
# shutdown, otherwise the buffered rows are discarded after the destination table has shut down.
# Note: $CLICKHOUSE_LOCAL is not used here because it passes --tmp.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"
mkdir -p "$data_dir"

local_path()
{
    ${CLICKHOUSE_BINARY} local --path "$data_dir" "$@"
}

local_path --query "
    CREATE DATABASE a;
    CREATE TABLE a.sink (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE buf (x UInt64) ENGINE = Buffer(a, sink, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
    CREATE TABLE broken (x UInt64) ENGINE = Set;
    INSERT INTO broken VALUES (1);
"

# The Set engine reads its persisted state back while the table loads. The data path is relative to `--path`.
broken_data_path=$(local_path --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'broken'")
for f in "$data_dir/${broken_data_path%/}"/*.bin; do echo garbage > "$f"; done

local_path --query "SELECT * FROM broken" 2>&1 | grep -o -m1 'ASYNC_LOAD_WAIT_FAILED'

local_path --query "
    INSERT INTO buf VALUES (42);
    SELECT count() FROM buf;
"

local_path --query "SELECT * FROM a.sink"

rm -rf "$data_dir"
