#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS d_memory_rollback"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_memory_rollback"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_memory_rollback (id UInt64, x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_rollback VALUES (1, 7)"
$CLICKHOUSE_CLIENT -q "CREATE DICTIONARY d_memory_rollback (id UInt64, x UInt8) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't_memory_rollback' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(0)"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_alter_pause_before_metadata_commit" >/dev/null 2>&1 || true' EXIT

# The `INSERT` commits while metadata persistence is paused. The rejected `ALTER` must keep that row.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT memory_alter_pause_before_metadata_commit"
alter_err=$(mktemp -p "${CLICKHOUSE_TMP}")
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_memory_rollback DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT dictGetUInt8(concat(currentDatabase(), '.d_memory_rollback'), 'x', id) + 1" >/dev/null 2>"$alter_err" &
alter_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT memory_alter_pause_before_metadata_commit PAUSE"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_memory_rollback VALUES (2, 42)" &
insert_pid=$!

for _ in $(seq 1 50); do
    running=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO t_memory_rollback%'")
    [ "$running" = "1" ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT memory_alter_pause_before_metadata_commit"
wait "$alter_pid" || true
wait "$insert_pid"
grep -o 'INFINITE_LOOP' "$alter_err" | head -1
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_memory_rollback ORDER BY id"
$CLICKHOUSE_CLIENT -q "SELECT default_expression = '' FROM system.columns WHERE database = currentDatabase() AND table = 't_memory_rollback' AND name = 'x'"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY d_memory_rollback"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_memory_rollback"
rm -f "$alter_err"

# An unmarked parent `CLEAR COLUMN`, as an older server persisted it, still clears `n.a`.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_legacy_parent_clear"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_legacy_parent_clear (id UInt8, \`n.a\` UInt8) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000, enable_block_number_column = 0, enable_block_offset_column = 0"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_legacy_parent_clear VALUES (1, 7)"
$CLICKHOUSE_CLIENT -q "SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_legacy_parent_clear' AND active"
data_path=$($CLICKHOUSE_CLIENT -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_legacy_parent_clear'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_legacy_parent_clear"
cat > "${data_path}/mutation_100000.txt" << 'EOF'
format version: 1
create time: 2026-11-09 00:00:00
commands: (CLEAR COLUMN n)
EOF
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_legacy_parent_clear"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_legacy_parent_clear FINAL"
$CLICKHOUSE_CLIENT -q "SELECT \`n.a\` FROM t_legacy_parent_clear"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_legacy_parent_clear"
