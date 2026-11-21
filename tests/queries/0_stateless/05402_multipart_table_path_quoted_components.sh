#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A back-quoted path component containing a dot spells the same name as the separate parts.

db=${CLICKHOUSE_DATABASE}
CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1"
spellings=("${db}.\`ns\`.t" "${db}.ns.t" "${db}.\`ns.t\`")

# Runs the query template with `@T` replaced by each spelling and checks that the outputs are identical.
compare()
{
    local label=$1 template=$2 outputs=() s
    for s in "${spellings[@]}"; do
        outputs+=("$($CLIENT -q "${template//@T/$s}" 2>&1)")
    done
    if [ -n "${outputs[0]}" ] && [ "${outputs[0]}" == "${outputs[1]}" ] && [ "${outputs[0]}" == "${outputs[2]}" ]; then
        echo "$label: same"
    else
        echo "$label: different"
        printf '%s\n---\n' "${outputs[@]}"
    fi
}

# Runs a statement without output once per spelling and prints `ok` or the error code.
each()
{
    local template=$1 s
    for s in "${spellings[@]}"; do
        $CLIENT -q "${template//@T/$s}" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1 | grep . || echo ok
    done
}

error_code()
{
    "$@" 2>&1 | grep -oE 'Code: [0-9]+' | head -n1
}

$CLIENT -q "CREATE TABLE ${db}.\`ns.t\` (x Int32) ENGINE = MergeTree ORDER BY x"

echo '-- INSERT'
each "INSERT INTO @T VALUES (1)"
$CLIENT -q "SELECT count() FROM ${db}.\`ns.t\`"

echo '-- statements with output'
compare 'select' "SELECT count(), sum(x) FROM @T"
$CLIENT -q "SELECT count(), sum(x) FROM ${db}.ns.t"
compare 'exists' "EXISTS TABLE @T"
$CLIENT -q "EXISTS TABLE ${db}.ns.t"

echo '-- OPTIMIZE, CHECK'
each "OPTIMIZE TABLE @T FINAL"
$CLIENT -q "SELECT count() FROM system.parts WHERE database = '${db}' AND table = 'ns.t' AND active"
compare 'check' "CHECK TABLE @T"

echo '-- ALTER'
$CLIENT -q "ALTER TABLE ${db}.\`ns\`.t ADD COLUMN y1 Int32"
$CLIENT -q "ALTER TABLE ${db}.ns.t ADD COLUMN y2 Int32"
$CLIENT -q "ALTER TABLE ${db}.\`ns.t\` ADD COLUMN y3 Int32"
$CLIENT -q "SELECT name FROM system.columns WHERE database = '${db}' AND table = 'ns.t' ORDER BY position"

compare 'describe' "DESCRIBE @T"
compare 'show columns' "SHOW COLUMNS FROM @T"
compare 'show indexes' "SHOW INDEXES FROM @T"
compare 'show create' "SHOW CREATE TABLE @T"

echo '-- SYSTEM'
# `OPTIMIZE` through the canonical name is aborted while merges are stopped through the spelling.
for s in "${spellings[@]}"; do
    $CLIENT -q "INSERT INTO ${db}.\`ns.t\` (x) VALUES (2)"
    $CLIENT -q "SYSTEM STOP MERGES $s"
    error_code $CLIENT -q "OPTIMIZE TABLE ${db}.\`ns.t\` FINAL SETTINGS optimize_throw_if_noop = 1"
    $CLIENT -q "SYSTEM START MERGES $s"
    $CLIENT -q "OPTIMIZE TABLE ${db}.\`ns.t\` FINAL SETTINGS optimize_throw_if_noop = 1"
done

echo '-- query parameter as the first part of a folded path'
$CLIENT --param_d="${db}" -q "EXISTS TABLE {d:Identifier}.ns.t"
$CLIENT --param_d="${db}" -q "INSERT INTO {d:Identifier}.ns.t VALUES (5, 0, 0, 0)"
$CLIENT -q "SELECT count() FROM ${db}.\`ns.t\` WHERE x = 5"

echo '-- RENAME'
$CLIENT -q "RENAME TABLE ${db}.ns.t TO ${db}.ns.t2"
$CLIENT -q "EXISTS TABLE ${db}.\`ns.t2\`"
$CLIENT -q "EXISTS TABLE ${db}.\`ns.t\`"
$CLIENT -q "RENAME TABLE ${db}.\`ns\`.t2 TO ${db}.\`ns.t\`"
$CLIENT -q "EXISTS TABLE ${db}.ns.t"

echo '-- DROP'
for s in "${spellings[@]}"; do
    $CLIENT -q "CREATE TABLE IF NOT EXISTS ${db}.\`ns.t\` (x Int32) ENGINE = MergeTree ORDER BY x"
    $CLIENT -q "DROP TABLE $s"
    $CLIENT -q "EXISTS TABLE ${db}.\`ns.t\`"
done

echo '-- database with a dot in its name'
ddb="\`${db}_x.d\`"
$CLIENT -q "DROP DATABASE IF EXISTS ${ddb}"
$CLIENT -q "CREATE DATABASE ${ddb}"
$CLIENT -q "CREATE TABLE ${ddb}.\`ns.t\` (x Int32) ENGINE = MergeTree ORDER BY x"
$CLIENT -q "INSERT INTO ${ddb}.ns.t VALUES (7)"
$CLIENT -q "EXISTS TABLE ${ddb}.ns.t"
$CLIENT -q "SELECT x FROM ${ddb}.ns.t"
$CLIENT -q "DROP DATABASE ${ddb}"

echo '-- formatting'
$CLIENT -q "SELECT formatQuerySingleLine('INSERT INTO a.\`b.c\`.d VALUES')"

echo '-- query parameter inside a path'
error_code $CLIENT --param_p=ns -q "SELECT * FROM ${db}.{p:Identifier}.t"
