#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A multipart table path (`db.ns.t`) in a table expression needs `allow_experimental_table_namespaces`, in stored definitions too.

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

echo '-- A: a table path in a table expression needs the setting'
run A1 "$OFF" "SELECT x FROM ${db}.ns.t"
run A2 "$OFF" "SELECT count() FROM ${db}.\`ns.t\` AS a JOIN ${db}.ns.t AS b USING x"
run A3 "$OFF" "SELECT x FROM ${db}.\`ns\`.t"

echo '-- T: statements that name their target accept the path'
run T1 "$OFF" "INSERT INTO ${db}.ns.u VALUES (3)"
run T2 "$OFF" "EXISTS TABLE ${db}.ns.t"

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
run S7d "$ON" "SELECT * FROM view(SELECT x FROM ${db}.ns.t)"
run S8a "$OFF" "INSERT INTO ${db}.\`ns.u\` SELECT x FROM ${db}.ns.t"
run S8b "$OFF" "CREATE TABLE ${db}.c2 ENGINE = Memory AS SELECT * FROM ${db}.ns.t"
run S8c "$OFF" "DESCRIBE (SELECT x FROM ${db}.ns.t)"
run S9 "$OFF" "SELECT 1 IN ${db}.ns.t"
run S10 "$ON" "SELECT 1 IN ${db}.ns.t"

echo '-- V: stored definitions'
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "CREATE VIEW ${db}.v AS SELECT x FROM ${db}.ns.t"
$OFF -q "SHOW CREATE TABLE ${db}.v" 2>&1 | sed "s/${db}/db/g"
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "CREATE MATERIALIZED VIEW ${db}.mv TO ${db}.ns.u AS SELECT x + 100 AS x FROM ${db}.plain"
run V2a "$OFF" "INSERT INTO ${db}.plain VALUES (1)"
run V2b "$OFF" "SELECT count() FROM ${db}.\`ns.u\` WHERE x = 101"
run D1 "$CLICKHOUSE_CLIENT --check_referential_table_dependencies=1" "DROP TABLE ${db}.\`ns.t\`"

echo '-- C: a definition with the SETTINGS clause does not depend on the setting of its user'
S="SETTINGS allow_experimental_table_namespaces = 1"
run C1a "$OFF" "CREATE VIEW ${db}.vc AS SELECT ns.t.x FROM ${db}.ns.t $S"
run C1b "$OFF" "SELECT * FROM ${db}.vc"
run C1c "$OFF" "DETACH TABLE ${db}.vc"
run C1d "$OFF" "ATTACH TABLE ${db}.vc"
run C1e "$OFF" "SELECT * FROM ${db}.vc"
run C2a "$OFF" "CREATE MATERIALIZED VIEW ${db}.mvc TO ${db}.plain AS SELECT ns.t.x + 200 AS x FROM ${db}.ns.t $S"
run C2b "$OFF" "INSERT INTO ${db}.\`ns.t\` VALUES (5)"
run C2c "$OFF" "SELECT count() FROM ${db}.plain WHERE x = 205"
run C3a "$OFF" "CREATE FUNCTION f_${db} AS () -> (SELECT count() FROM ${db}.ns.t WHERE ns.t.x > 0 $S)"
run C3b "$OFF" "SELECT f_${db}()"
$CLICKHOUSE_CLIENT -q "DROP FUNCTION f_${db}"
run C4a "$OFF" "CREATE TABLE ${db}.ttl_c (id UInt64, d DateTime) ENGINE = MergeTree ORDER BY id TTL d WHERE id IN (SELECT x FROM ${db}.ns.t $S)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.ttl_a (id UInt64, d DateTime) ENGINE = MergeTree ORDER BY id"
run C4b "$OFF" "ALTER TABLE ${db}.ttl_a MODIFY TTL d WHERE id IN (SELECT x FROM ${db}.ns.t $S)"

echo '-- F: formatting is not checked'
run F1a "$OFF" "SELECT formatQuerySingleLine('CREATE TABLE a.b.c (x UInt64) ENGINE = Memory')"
run F1b "$OFF" "SELECT formatQuerySingleLine('SELECT * FROM a.b.c')"
run F1c "$OFF" "SELECT formatQuerySingleLine('INSERT INTO a.b.c VALUES')"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.ttl_c"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.ttl_a"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.mvc"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.vc"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.v"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.u\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.plain"
