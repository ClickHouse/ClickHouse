#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="db_mcl_${CLICKHOUSE_DATABASE}_$$_${RANDOM}"
DB_RENAMED="${DB}_renamed"

$CLICKHOUSE_CLIENT -m --query "
DROP DATABASE IF EXISTS $DB;
DROP DATABASE IF EXISTS $DB_RENAMED;
CREATE DATABASE $DB ENGINE = Atomic COMMENT 'analytics db';
CREATE TABLE $DB.orders (
    id UInt64 COMMENT 'order id',
    org_id UInt64,
    amount Decimal(10, 2) DEFAULT 0,
    amount_cents Int64 MATERIALIZED toInt64(amount * 100),
    created_at DateTime
) ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (org_id, id)
COMMENT 'orders v1';

INSERT INTO $DB.orders SETTINGS async_insert = 0 VALUES (1, 10, 12.50, '2026-01-01 00:00:00');
SELECT count() FROM $DB.orders FORMAT Null;

ALTER TABLE $DB.orders ADD COLUMN currency LowCardinality(String) DEFAULT 'USD' COMMENT 'iso currency';
ALTER TABLE $DB.orders MODIFY COMMENT 'orders v2';

RENAME TABLE $DB.orders TO $DB.orders_v2;
DROP TABLE $DB.orders_v2;
RENAME DATABASE $DB TO $DB_RENAMED;
DROP DATABASE $DB_RENAMED;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS metadata_change_log"

echo '--- event sequence ---'
$CLICKHOUSE_CLIENT --query "
SELECT
    event_type,
    if(database = '$DB', 'db_mcl', if(database = '$DB_RENAMED', 'db_mcl_renamed', database)) AS db,
    table,
    if(previous_database = '$DB', 'db_mcl', previous_database) AS prev_db,
    previous_table,
    engine,
    comment
FROM system.metadata_change_log
WHERE database IN ('$DB', '$DB_RENAMED') OR previous_database = '$DB'
ORDER BY event_time_microseconds ASC,
    multiIf(
        event_type = 'DatabaseCreate', 1,
        event_type = 'TableCreate', 2,
        event_type = 'TableAlter', 3,
        event_type = 'TableRename', 4,
        event_type = 'TableDrop', 5,
        event_type = 'DatabaseRename', 6,
        event_type = 'DatabaseDrop', 7,
        8
    ) ASC,
    comment ASC
"

echo '--- rename includes normalized CREATE query ---'
$CLICKHOUSE_CLIENT --query "
SELECT startsWith(create_query, 'CREATE TABLE ')
FROM system.metadata_change_log
WHERE database = '$DB' AND table = 'orders_v2' AND event_type = 'TableRename'
"

echo '--- columns after alter ---'
$CLICKHOUSE_CLIENT --query "
SELECT
    columns_name,
    columns_type,
    columns_default_kind,
    columns_comment
FROM system.metadata_change_log
WHERE database = '$DB' AND table = 'orders' AND event_type = 'TableAlter'
ORDER BY event_time_microseconds DESC, comment DESC
LIMIT 1
"

echo '--- uuid preserved across rename ---'
$CLICKHOUSE_CLIENT --query "
SELECT
    uniqExact(table_uuid) = 1 AS same_uuid,
    min(table_uuid) != toUUID('00000000-0000-0000-0000-000000000000') AS non_nil_uuid
FROM system.metadata_change_log
WHERE database = '$DB' AND table IN ('orders', 'orders_v2') AND event_type IN ('TableCreate', 'TableRename', 'TableDrop')
"

echo '--- internal databases excluded ---'
$CLICKHOUSE_CLIENT --query "
SELECT count() = 0 AS no_internal
FROM system.metadata_change_log
WHERE database IN ('system', 'INFORMATION_SCHEMA', 'information_schema', '_temporary_and_external_tables')
"
