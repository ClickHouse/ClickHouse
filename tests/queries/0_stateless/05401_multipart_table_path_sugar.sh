#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


db=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x Int32) ENGINE = MergeTree ORDER BY x"

echo '-- setting off'
$CLICKHOUSE_CLIENT -q "INSERT INTO ${db}.ns.t VALUES (1)"
$CLICKHOUSE_CLIENT -q "SELECT x FROM ${db}.ns.t"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${db}.\`ns.t\`"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${db}.ns.t"
$CLICKHOUSE_CLIENT -q "DESCRIBE ${db}.ns.t" | cut -f1-2
$CLICKHOUSE_CLIENT -q "SHOW COLUMNS FROM ${db}.ns.t" | cut -f1-2
$CLICKHOUSE_CLIENT -q "SHOW INDEXES FROM ${db}.ns.t" | cut -f1 | uniq

a=$($CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE ${db}.ns.t")
b=$($CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE ${db}.\`ns.t\`")
[ "$a" == "$b" ] && echo 'show create: same' || echo 'show create: different'
a=$($CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX oneline = 1 SELECT x FROM ${db}.ns.t")
b=$($CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX oneline = 1 SELECT x FROM ${db}.\`ns.t\`")
[ "$a" == "$b" ] && echo 'explain syntax: same' || echo 'explain syntax: different'

$CLICKHOUSE_CLIENT -q "SELECT formatQuerySingleLine('SELECT * FROM a.b.c')"

echo '-- DDL'
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.ns.t2 (x Int32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM ${db} LIKE 'ns.%'"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.ns.t2"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${db}.\`ns.t2\`"

echo '-- errors, setting off'
$CLICKHOUSE_CLIENT -q "USE ${db}.ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM ${db}.ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT -q "SHOW DICTIONARIES FROM ${db}.ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT -q "SHOW TABLES FROM \`${db}.ns\`" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${db}.\`ns.x\`.t" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT --param_p=ns -q "SELECT * FROM ${db}.{p:Identifier}.t" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT -q "SELECT ${db}.ns.t.x FROM ${db}.ns.t SETTINGS enable_analyzer = 1" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1

echo '-- HTTP'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT x FROM ${db}.ns.t"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT x FROM ${db}.ns.t SETTINGS allow_experimental_table_namespaces = 1"

echo '-- setting on'
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "SELECT x FROM ${db}.ns.t"
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "USE ${db}.ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 -q "SHOW TABLES FROM ${db}.ns" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${db}.\`ns.t\`"
