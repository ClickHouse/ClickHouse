#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# An INSERT still on the UInt64 schema must apply the same default conversion as a stored block.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_memory_default_inflight"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_memory_default_inflight (a UInt64, x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_default_inflight VALUES (257, 7)"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_sink_pause_before_commit" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT memory_sink_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_default_inflight VALUES (258, 8)" &
insert_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT memory_sink_pause_before_commit PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_default_inflight MODIFY COLUMN a UInt8"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_default_inflight DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a + 1"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_sink_pause_before_commit"
wait "$insert_pid"

$CLICKHOUSE_CLIENT -q "SELECT a, x FROM t_memory_default_inflight ORDER BY a"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_memory_default_inflight"
