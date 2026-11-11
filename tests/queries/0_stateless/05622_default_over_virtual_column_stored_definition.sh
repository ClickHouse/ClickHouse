#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table stored by an affected version may already have a default over a virtual column. Loading it
# and an unrelated ALTER on it must still work; the stricter check applies only to new and modified
# columns. The stored definition is rewritten to carry such a default and then attached back.

$CLICKHOUSE_CLIENT --query "CREATE TABLE legacy (c0 String MATERIALIZED 'x', c1 UInt16) ENGINE = MergeTree ORDER BY tuple()"
metadata_file=$($CLICKHOUSE_CLIENT --query "
    SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'legacy'")
$CLICKHOUSE_CLIENT --query "DETACH TABLE legacy"
sed -i.bak "s/MATERIALIZED 'x'/MATERIALIZED _table/" "$metadata_file" && rm -f "$metadata_file.bak"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE legacy"
$CLICKHOUSE_CLIENT --query "SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 'legacy' AND name = 'c0'"
$CLICKHOUSE_CLIENT --query "ALTER TABLE legacy ADD COLUMN c2 UInt8 DEFAULT 0"
$CLICKHOUSE_CLIENT --query "SELECT 'legacy attach and unrelated alter ok'"
$CLICKHOUSE_CLIENT --query "DROP TABLE legacy"
