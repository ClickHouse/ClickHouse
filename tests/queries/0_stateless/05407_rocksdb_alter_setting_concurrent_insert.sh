#!/usr/bin/env bash
# Tags: use-rocksdb

# `ALTER TABLE ... MODIFY SETTING` on an `EmbeddedRocksDB` table runs concurrently with `INSERT`s, which read the settings
# (`optimize_for_bulk_insert`, `bulk_insert_block_size`). `getSettings` used to return a reference into a snapshot it
# dropped, so the `ALTER` could free the settings under the readers: a data race that ThreadSanitizer reports
# (https://github.com/ClickHouse/ClickHouse/issues/126587).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_rdb (k UInt64, v UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k"

function insert_rows()
{
    local offset=$1
    for i in $(seq 0 299); do
        echo "INSERT INTO t_rdb SELECT $offset + $i * 10 + number, number FROM numbers(10);"
    done | $CLICKHOUSE_CLIENT
}

function alter_settings()
{
    for i in $(seq 300); do
        echo "ALTER TABLE t_rdb MODIFY SETTING optimize_for_bulk_insert = $((i % 2)), bulk_insert_block_size = $((1000 + i % 7));"
    done | $CLICKHOUSE_CLIENT
}

insert_rows 0 &
insert_rows 100000 &
alter_settings &
wait

# Every row is inserted, whichever settings each `INSERT` used.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_rdb"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_rdb"
