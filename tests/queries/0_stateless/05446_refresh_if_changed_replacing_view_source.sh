#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.
# `REFRESH ... IF CHANGED` in the replacing mode reading through an `SQL SECURITY INVOKER` view: the view
# folds the settings it is read with into its hash, and a replacing refresh zeroes
# `max_table_size_to_drop` / `max_partition_size_to_drop` between its two source hashes. Those settings
# never change a row, so scheduled refreshes over an unchanged source must keep being skipped: the target
# table, which a refresh that runs replaces, keeps its UUID.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_05446 SYNC;
    DROP TABLE IF EXISTS v_05446;
    DROP TABLE IF EXISTS src_05446 SYNC;
    CREATE TABLE src_05446 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_05446 VALUES (1);
    CREATE VIEW v_05446 SQL SECURITY INVOKER AS SELECT x FROM src_05446;
    CREATE MATERIALIZED VIEW mv_05446 REFRESH EVERY 1 SECOND IF CHANGED
        ENGINE = MergeTree ORDER BY x AS SELECT x FROM v_05446;
"

target_uuid()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT uuid FROM system.tables
        WHERE database = currentDatabase()
            AND name = '.inner_id.' || toString((SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'mv_05446'))"
}

for _ in {1..120}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_05446")" = "1" ] && break
    sleep 0.5
done
# Let the refresh that follows the first one record its watermark.
sleep 2
before=$(target_uuid)

# Several scheduled refreshes over the unchanged source.
sleep 3
after=$(target_uuid)
[ -n "$before" ] && [ "$before" = "$after" ] && echo "unchanged source keeps the target: yes" || echo "unchanged source keeps the target: no ($before, $after)"

$CLICKHOUSE_CLIENT -q "INSERT INTO src_05446 VALUES (2)"
for _ in {1..120}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_05446")" = "2" ] && break
    sleep 0.5
done
echo "changed source: $($CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM (SELECT x FROM mv_05446 ORDER BY x)")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05446 SYNC;
    DROP TABLE v_05446;
    DROP TABLE src_05446 SYNC;
"
