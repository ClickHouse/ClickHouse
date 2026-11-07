#!/usr/bin/env bash
# Tags: shard

# Tables whose database and table names joined by a dot give the same string are different tables
# for scalar subqueries and IN sets of one query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

D=${CLICKHOUSE_DATABASE}
A="${D}.\`inner.t\`"
B="\`${D}.inner\`.t"

function drop_databases()
{
    $CLICKHOUSE_CLIENT -q "
        DROP DATABASE IF EXISTS \`${D}.inner\`;
        DROP DATABASE IF EXISTS \`${D}.p\`;
        DROP DATABASE IF EXISTS \`${D}.p.q\`;
    "
}

drop_databases

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE \`${D}.inner\`;
    CREATE TABLE $A (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $A VALUES (1);
    CREATE TABLE $B (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $B VALUES (0), (2);
"

$CLICKHOUSE_CLIENT -q "
    SELECT 'scalar', (SELECT max(x) FROM $A), (SELECT max(x) FROM $B);
    SELECT 'scalar reversed', (SELECT max(x) FROM $B), (SELECT max(x) FROM $A);
    SELECT 'array scalar', (SELECT arraySort(groupArray(x)) FROM $A), (SELECT arraySort(groupArray(x)) FROM $B);
    SELECT 'in', 2 IN (SELECT x FROM $A), 2 IN (SELECT x FROM $B);
    SELECT 'where in or in', count() FROM numbers(3) WHERE number IN (SELECT x FROM $A) OR number IN (SELECT x FROM $B);
    SELECT 'group by', number IN (SELECT x FROM $A) AS a, number IN (SELECT x FROM $B) AS b, count()
    FROM numbers(3) GROUP BY a, b ORDER BY a, b;
    SELECT 'remote', countIf(number IN (SELECT x FROM $A)), countIf(number IN (SELECT x FROM $B))
    FROM remote('127.0.0.{1,2}', numbers(3));
"

$CLICKHOUSE_CLIENT -q "
    CREATE DATABASE \`${D}.p\`;
    CREATE DATABASE \`${D}.p.q\`;
    CREATE TABLE \`${D}.p\`.\`q.s\` (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`${D}.p\`.\`q.s\` VALUES (1);
    CREATE TABLE \`${D}.p.q\`.s (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`${D}.p.q\`.s VALUES (2);
    SELECT 'two dotted databases', (SELECT max(x) FROM \`${D}.p\`.\`q.s\`), (SELECT max(x) FROM \`${D}.p.q\`.s);
"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${D}.tt (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${D}.tt VALUES (2);
    CREATE TEMPORARY TABLE \`${D}.tt\` (x UInt64) ENGINE = Memory;
    INSERT INTO \`${D}.tt\` VALUES (1);
    SELECT 'temporary table scalar', (SELECT max(x) FROM \`${D}.tt\`), (SELECT max(x) FROM ${D}.tt);
    SELECT 'temporary table in', 2 IN (SELECT x FROM \`${D}.tt\`), 2 IN (SELECT x FROM ${D}.tt);
"

drop_databases
