#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
# no-shared-merge-tree: the table engine is replaced.

# The first access to a table that is not loaded yet races with `DETACH TABLE`. Loading the table analyzes a
# constraint with a scalar subquery, which looks up a table in the same database and takes the database mutex,
# while `DETACH` holds the database mutex and asks the table for its engine name to update the metrics.
# Asking for the name must not wait for the load, otherwise both queries hang.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS ${DB};
    CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${DB}.ref (x UInt64) ENGINE = Memory;
    CREATE TABLE ${DB}.t (id UInt64, CONSTRAINT c CHECK id < (SELECT count() + 100 FROM ${DB}.ref)) ENGINE = MergeTree ORDER BY id;
"

for _ in {1..30}
do
    # A detached table is attached again with the database, and is not loaded until the first access.
    $CLICKHOUSE_CLIENT -m -q "DETACH DATABASE ${DB} SYNC; ATTACH DATABASE ${DB};"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM ${DB}.t FORMAT Null" 2>/dev/null &
    $CLICKHOUSE_CLIENT -q "DETACH TABLE ${DB}.t SYNC" &
    wait
done

$CLICKHOUSE_CLIENT -m -q "
    DETACH DATABASE ${DB} SYNC;
    ATTACH DATABASE ${DB};
    SELECT name, is_loaded FROM system.tables WHERE database = '${DB}' ORDER BY name;
    SELECT count() FROM ${DB}.t;
    DROP DATABASE ${DB} SYNC;
"
