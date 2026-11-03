#!/usr/bin/env bash
# Tags: memory-engine

# `ALTER TABLE ... MODIFY SETTING` on a `Memory` table runs concurrently with `INSERT`s, which read the settings
# (`compress`, `max_rows_to_keep`, `min_rows_to_keep`). The `ALTER` used to replace the settings object in place,
# freeing it under the readers: a data race that ThreadSanitizer reports (https://github.com/ClickHouse/ClickHouse/issues/126586).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_mem (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 1000"

function insert_rows()
{
    for _ in $(seq 300); do
        echo "INSERT INTO t_mem SELECT number FROM numbers(100);"
    done | $CLICKHOUSE_CLIENT
}

function alter_settings()
{
    for i in $(seq 300); do
        echo "ALTER TABLE t_mem MODIFY SETTING max_rows_to_keep = $((1000 + i % 2 * 500)), min_rows_to_keep = $((i % 2)), compress = $((i % 2));"
    done | $CLICKHOUSE_CLIENT
}

insert_rows &
insert_rows &
alter_settings &
wait

# The table applies the settings of the last `ALTER`.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_mem MODIFY SETTING max_rows_to_keep = 100, min_rows_to_keep = 0, compress = 0"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_mem"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_mem SELECT number FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_mem"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_mem"
