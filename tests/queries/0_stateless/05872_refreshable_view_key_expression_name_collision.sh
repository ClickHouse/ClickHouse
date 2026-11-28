#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-replicated-database: a refreshable view with a non-replicated target is refused in a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A refreshable view whose target is ordered by an expression named like one of its columns keeps refreshing
# when the target definition has no `allow_suspicious_indices`, as for a view created by an earlier version.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rmv;
    DROP TABLE IF EXISTS src;
    CREATE TABLE src (s String) ENGINE = MergeTree ORDER BY s;
    INSERT INTO src VALUES ('HeLLo');
    CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR ENGINE = MergeTree ORDER BY lower(s) SETTINGS allow_suspicious_indices = 1
        AS SELECT s, lower(s) FROM src;
    SYSTEM WAIT VIEW rmv;
"

inner=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'")
$CLICKHOUSE_CLIENT -q "ALTER TABLE \`$inner\` RESET SETTING allow_suspicious_indices"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '$inner' AND create_table_query LIKE '%allow_suspicious_indices%'"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO src VALUES ('WoRLD');
    SYSTEM REFRESH VIEW rmv;
    SYSTEM WAIT VIEW rmv;
"
$CLICKHOUSE_CLIENT -q "SELECT * FROM rmv ORDER BY s"
$CLICKHOUSE_CLIENT -q "SELECT exception FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'rmv'"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rmv;
    DROP TABLE src;
"
