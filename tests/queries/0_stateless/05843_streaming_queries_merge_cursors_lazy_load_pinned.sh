#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

# An incremental refresh from a table of a database with `lazy_load_tables`, and from a `Merge` table over it,
# while another query holds the lazy stand-in of the table. The stand-in stays in the database as long as it is
# in use, so it has to report the same capabilities as the loaded table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

LAZY_DB=$CLICKHOUSE_DATABASE_1

$STREAMING_CLIENT -q "
    DROP DATABASE IF EXISTS $LAZY_DB;
    CREATE DATABASE $LAZY_DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE $LAZY_DB.src (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS $STREAMING_TABLE_SETTINGS;
    CREATE TABLE $LAZY_DB.m (a UInt64) ENGINE = Merge('$LAZY_DB', '^src\$');
    CREATE TABLE $LAZY_DB.tgt_direct (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE $LAZY_DB.tgt_merge (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO $LAZY_DB.src VALUES (1), (2);
    DETACH DATABASE $LAZY_DB;
    ATTACH DATABASE $LAZY_DB;
"

echo "--- a streaming reader holds the lazy stand-in of the table"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT 'reader ' || a FROM $LAZY_DB.src STREAM")
read_until "$fifo" "reader 2" > /dev/null

echo "--- incremental refresh from the table"
$STREAMING_CLIENT -q "
    CREATE MATERIALIZED VIEW $LAZY_DB.mv_direct REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO $LAZY_DB.tgt_direct EMPTY AS SELECT a FROM $LAZY_DB.src;
    SYSTEM REFRESH VIEW $LAZY_DB.mv_direct;
    SYSTEM WAIT VIEW $LAZY_DB.mv_direct;
    SELECT arraySort(groupArray(a)) FROM $LAZY_DB.tgt_direct;
"

echo "--- incremental refresh from a Merge table over it"
$STREAMING_CLIENT -q "
    CREATE MATERIALIZED VIEW $LAZY_DB.mv_merge REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO $LAZY_DB.tgt_merge EMPTY AS SELECT a FROM $LAZY_DB.m;
    SYSTEM REFRESH VIEW $LAZY_DB.mv_merge;
    SYSTEM WAIT VIEW $LAZY_DB.mv_merge;
    SELECT arraySort(groupArray(a)) FROM $LAZY_DB.tgt_merge;
"
$STREAMING_CLIENT -q "INSERT INTO $LAZY_DB.src VALUES (3)"
$STREAMING_CLIENT -q "
    SYSTEM REFRESH VIEW $LAZY_DB.mv_merge;
    SYSTEM WAIT VIEW $LAZY_DB.mv_merge;
    SELECT arraySort(groupArray(a)) FROM $LAZY_DB.tgt_merge;
"

echo "--- the reader is still running"
read_until "$fifo" "reader 3"

cleanup "$fifo" "$pid" > /dev/null

$STREAMING_CLIENT -q "DROP DATABASE $LAZY_DB"
