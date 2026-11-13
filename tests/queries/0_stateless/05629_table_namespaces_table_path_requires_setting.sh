#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A multipart table path (`db.ns.t`) is rejected with SUPPORT_IS_DISABLED unless `allow_experimental_table_namespaces` is on.

db=${CLICKHOUSE_DATABASE}
OFF="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=0"
ON="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1"

# Prints the label and the first error code, or the output (newlines joined), or `ok` for no output.
run()
{
    local label=$1 client=$2 query=$3 out
    out=$($client -q "$query" 2>&1)
    if [[ "$out" =~ (Code:\ [0-9]+) ]]; then
        echo "$label: ${BASH_REMATCH[1]}"
    else
        echo "$label: ${out:-ok}" | tr '\n' ' ' | sed 's/ $//'
        echo
    fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.u\` (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.plain (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.\`ns.t\` VALUES (1)"

echo '-- A: every fold site is rejected'
run A1 "$OFF" "SELECT x FROM ${db}.ns.t"
run A2 "$OFF" "SELECT count() FROM ${db}.\`ns.t\` AS a JOIN ${db}.ns.t AS b USING x"
run A3 "$OFF" "SELECT x FROM ${db}.\`ns\`.t"
run A4 "$OFF" "DESCRIBE ${db}.ns.t"
run A5a "$OFF" "DROP TABLE ${db}.ns.t"
run A5b "$OFF" "DETACH TABLE ${db}.ns.t"
run A5c "$OFF" "TRUNCATE TABLE ${db}.ns.t"
run A5d "$OFF" "DROP TABLE ${db}.plain, ${db}.ns.t"
run A6 "$OFF" "CREATE MATERIALIZED VIEW ${db}.mv REFRESH EVERY 1 HOUR DEPENDS ON ${db}.ns.t ENGINE = Memory AS SELECT 1 AS x"
run A7a "$OFF" "CREATE TABLE ${db}.ns.c (x UInt64) ENGINE = Memory"
run A7b "$OFF" "CREATE VIEW ${db}.ns.v AS SELECT 1"
run A7c "$OFF" "CREATE DICTIONARY ${db}.ns.d (x UInt64, y UInt64 DEFAULT 0) PRIMARY KEY x SOURCE(CLICKHOUSE(DB '${db}' TABLE 'ns.t')) LAYOUT(FLAT()) LIFETIME(0)"
run A7d "$OFF" "ATTACH TABLE ${db}.ns.t"
run A8 "$OFF" "INSERT INTO ${db}.ns.t VALUES (2)"
run A9a "$OFF" "ALTER TABLE ${db}.ns.t ADD COLUMN y UInt64"
run A9b "$OFF" "DELETE FROM ${db}.ns.t WHERE x = 5"
run A9c "$OFF" "UPDATE ${db}.ns.t SET x = 2 WHERE x = 5"
run A10a "$OFF" "OPTIMIZE TABLE ${db}.ns.t"
run A10b "$OFF" "CHECK TABLE ${db}.ns.t"
run A10c "$OFF" "EXISTS TABLE ${db}.ns.t"
run A10d "$OFF" "SHOW CREATE TABLE ${db}.ns.t"
run A11a "$OFF" "RENAME TABLE ${db}.ns.t TO ${db}.plain2"
run A11b "$OFF" "RENAME TABLE ${db}.\`ns.t\` TO ${db}.ns.t9"
run A11c "$OFF" "EXCHANGE TABLES ${db}.\`ns.t\` AND ${db}.ns.u"
run A12a "$OFF" "UNDROP TABLE ${db}.ns.t"
run A12b "$OFF" "CREATE INDEX i ON ${db}.ns.t (x) TYPE minmax"
run A12c "$OFF" "DROP INDEX i ON ${db}.ns.t"
run A13 "$OFF" "SYSTEM STOP MERGES ${db}.ns.t"
run A14a "$OFF" "SHOW COLUMNS FROM ${db}.ns.t"
run A14b "$OFF" "SHOW INDEXES FROM ${db}.ns.t"
run A15 "$OFF" "CREATE MATERIALIZED VIEW ${db}.mv TO ${db}.ns.u AS SELECT x FROM ${db}.plain"

echo '-- nothing was executed'
run A_count "$CLICKHOUSE_CLIENT" "SELECT count() FROM ${db}.\`ns.t\`"
run A_exists_u "$CLICKHOUSE_CLIENT" "EXISTS TABLE ${db}.\`ns.u\`"
run A_exists_plain "$CLICKHOUSE_CLIENT" "EXISTS TABLE ${db}.plain"

echo '-- S: SELECT scoping'
run S1 "$OFF" "SELECT x FROM ${db}.\`ns.t\`"
run S2 "$OFF" "SELECT x FROM ${db}.ns.t SETTINGS allow_experimental_table_namespaces = 1"
run S3 "$OFF" "INSERT INTO ${db}.ns.u SETTINGS allow_experimental_table_namespaces = 1 VALUES (7)"
run S4 "$OFF" "SELECT * FROM (SELECT x FROM ${db}.ns.t)"
run S5a "$OFF" "SELECT * FROM (SELECT x FROM ${db}.ns.t SETTINGS allow_experimental_table_namespaces = 1)"
run S5b "$OFF" "SELECT * FROM (SELECT x FROM ${db}.ns.t SETTINGS allow_experimental_table_namespaces = 0) SETTINGS allow_experimental_table_namespaces = 1"
run S6a "$OFF" "SELECT x FROM ${db}.ns.t UNION ALL SELECT x FROM ${db}.ns.t"
run S6b "$OFF" "SELECT * FROM (SELECT x FROM ${db}.ns.t UNION ALL SELECT x FROM ${db}.ns.t SETTINGS allow_experimental_table_namespaces = 1)"
run S7a "$OFF" "WITH c AS (SELECT x FROM ${db}.ns.t) SELECT * FROM c"
run S7b "$OFF" "SELECT 1 IN (SELECT x FROM ${db}.ns.t)"
run S7c "$OFF" "SELECT * FROM view(SELECT x FROM ${db}.ns.t)"
run S8a "$OFF" "INSERT INTO ${db}.\`ns.u\` SELECT x FROM ${db}.ns.t"
run S8b "$OFF" "CREATE TABLE ${db}.c2 ENGINE = Memory AS SELECT * FROM ${db}.ns.t"
run S8c "$OFF" "DESCRIBE (SELECT x FROM ${db}.ns.t)"
run S8d "$OFF" "EXPLAIN SYNTAX SELECT x FROM ${db}.ns.t"
run S8e "$OFF" "ALTER TABLE ${db}.\`ns.u\` DELETE WHERE x IN (SELECT x FROM ${db}.ns.t) SETTINGS mutations_sync = 1"
run S9 "$OFF" "SELECT 1 IN ${db}.ns.t"
run S10 "$ON" "SELECT 1 IN ${db}.ns.t"

echo '-- P: subqueries in `SHOW ... WHERE` and row policy conditions'
run P1a "$OFF" "SHOW TABLES FROM ${db} WHERE name IN (SELECT 'plain' FROM ${db}.ns.t)"
run P1b "$OFF" "SHOW COLUMNS FROM ${db}.plain WHERE field IN (SELECT 'x' FROM ${db}.ns.t)"
run P1c "$OFF" "SHOW INDEXES FROM ${db}.plain WHERE 1 IN (SELECT x FROM ${db}.ns.t)"
run P1d "$OFF" "CREATE ROW POLICY rp_${db} ON ${db}.plain USING x IN (SELECT x FROM ${db}.ns.t) TO NONE"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY rp_${db} ON ${db}.plain USING x IN (SELECT x FROM ${db}.\`ns.t\`) TO NONE"
run P1e "$OFF" "ALTER ROW POLICY rp_${db} ON ${db}.plain USING x IN (SELECT x FROM ${db}.ns.t)"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY rp_${db} ON ${db}.plain"
run P2a "$OFF" "SHOW TABLES FROM ${db} WHERE name IN (SELECT 'plain' FROM ${db}.\`ns.t\`)"
run P2b "$OFF" "CREATE ROW POLICY rp2_${db} ON ${db}.plain USING x IN (SELECT x FROM ${db}.\`ns.t\`) TO NONE"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY rp2_${db} ON ${db}.plain"

echo '-- V: stored definitions'
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "CREATE VIEW ${db}.v AS SELECT x FROM ${db}.ns.t"
run V1a "$OFF" "SELECT * FROM ${db}.v"
$OFF -q "SHOW CREATE TABLE ${db}.v" 2>&1 | sed "s/${db}/db/g"
run V1b "$OFF" "DETACH TABLE ${db}.v"
run V1c "$OFF" "ATTACH TABLE ${db}.v"
run V1d "$OFF" "SELECT * FROM ${db}.v"
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "CREATE MATERIALIZED VIEW ${db}.mv TO ${db}.ns.u AS SELECT x + 100 AS x FROM ${db}.plain"
run V2a "$OFF" "INSERT INTO ${db}.plain VALUES (1)"
run V2b "$OFF" "SELECT count() FROM ${db}.\`ns.u\` WHERE x = 101"

echo '-- F: formatting is not checked'
run F1a "$OFF" "SELECT formatQuerySingleLine('CREATE TABLE a.b.c (x UInt64) ENGINE = Memory')"
run F1b "$OFF" "SELECT formatQuerySingleLine('SELECT * FROM a.b.c')"
run F1c "$OFF" "SELECT formatQuerySingleLine('INSERT INTO a.b.c VALUES')"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.v"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.u\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.plain"
