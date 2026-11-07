#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Pauses the in-flight `INSERT` on the server-wide failpoint.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_memory_race"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_memory_race (id UInt64, x UInt64) ENGINE = Memory"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_sink_pause_before_commit" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT memory_sink_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_race SELECT number, number FROM numbers(100)" &
insert_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT memory_sink_pause_before_commit PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_race RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT 999"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_sink_pause_before_commit"
wait "$insert_pid"

$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(x = 999), countIf(x_old = id) FROM t_memory_race"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_memory_race"

# An ADD between the paused INSERT and the rename must not hide the old sink from the rewrite.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_memory_race (id UInt64, x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT memory_sink_pause_before_commit"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_race SELECT number, number FROM numbers(100)" &
insert_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT memory_sink_pause_before_commit PAUSE"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_race ADD COLUMN y UInt64"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_race RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT 999"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_sink_pause_before_commit"
wait "$insert_pid"

$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(x = 999), countIf(x_old = id) FROM t_memory_race"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_memory_race"
