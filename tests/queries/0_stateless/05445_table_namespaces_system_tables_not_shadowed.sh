#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
gone=${db}_gone

ON="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`system.columns\` (x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`system.tables\` (x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`system.one\` (x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.t (a UInt64, b String, INDEX idx b TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY a"

echo '-- SHOW COLUMNS'
$ON -q "SHOW COLUMNS FROM t"
echo '-- SHOW INDEXES'
$ON -q "SHOW INDEXES FROM t"
echo '-- information_schema.columns'
$ON -q "SELECT count() FROM information_schema.columns WHERE table_schema = currentDatabase() AND table_name = 't'"
echo '-- system.columns'
$ON -q "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY name"
echo '-- system.one'
$ON -q "SELECT * FROM system.one"

echo '-- temporary table with a dotted name'
$ON -n -q "CREATE TEMPORARY TABLE \`system.one\` (secret UInt64); SELECT * FROM system.one"

echo '-- dropped current database'
$ON -n -q "CREATE DATABASE ${gone}; USE ${gone}; DROP DATABASE ${gone}; SELECT * FROM system.one"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`system.columns\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`system.tables\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`system.one\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.t"
