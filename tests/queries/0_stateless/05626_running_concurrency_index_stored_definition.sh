#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A secondary index over a non-deterministic function is refused in a new definition, but a table an
# earlier version stored with such an index still loads. The stored definition of a table with a
# deterministic index is rewritten and attached back with the short ATTACH.

$CLICKHOUSE_CLIENT -q "CREATE TABLE legacy_index (a UInt64, s DateTime, e DateTime, INDEX i s TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a"
metadata_file=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'legacy_index'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE legacy_index"
sed -i.bak 's/INDEX i s TYPE minmax/INDEX i runningConcurrency(s, e) TYPE minmax/' "$metadata_file" && rm -f "$metadata_file.bak"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE legacy_index"
$CLICKHOUSE_CLIENT -q "SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'legacy_index'"
$CLICKHOUSE_CLIENT -q "SELECT 'legacy index attaches', count() FROM legacy_index"
$CLICKHOUSE_CLIENT -q "DROP TABLE legacy_index"
