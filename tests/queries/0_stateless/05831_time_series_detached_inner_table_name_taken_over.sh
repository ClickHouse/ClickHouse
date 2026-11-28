#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "CREATE TABLE ts ENGINE = TimeSeries"
samples=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")
[ -n "$samples" ] || { echo "No samples table of ts"; exit 1; }

# The samples table is detached under another name, and an unrelated table takes over its generated name.
$CLICKHOUSE_CLIENT -q "
    RENAME TABLE \`$samples\` TO saved_samples;
    CREATE TABLE \`$samples\` (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`$samples\` VALUES (1);
    DETACH TABLE saved_samples;
"

# Neither TRUNCATE nor DROP of the `TimeSeries` table may touch the unrelated table.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ts" 2>&1 | grep -o "UNKNOWN_TABLE" | head -1
$CLICKHOUSE_CLIENT -q "SELECT count() FROM \`$samples\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE \`$samples\`"
