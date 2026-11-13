#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# The failpoint is process-wide. It pauses after the INSERT has its metadata and before the sink is built.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_memory_gen"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_memory_gen (id UInt64, x UInt64) ENGINE = Memory"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_write_pause_before_sink" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT memory_write_pause_before_sink"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_gen SELECT number, number FROM numbers(10)" &
insert_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT memory_write_pause_before_sink PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_gen RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT 999"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_write_pause_before_sink"
wait "$insert_pid"

$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(x = 999), countIf(x_old = id) FROM t_memory_gen"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_memory_gen"
