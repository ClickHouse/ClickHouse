#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires Iceberg (USE_AVRO build option) and S3

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in for the real storage, and an
# `Alias` refreshes the external metadata (e.g. the schema of an Iceberg table) only of a target that is already
# materialized. Here the external schema changes while the database is detached, and the first access after `ATTACH`
# goes through an `Alias`:
# - an `INSERT` into the source of a materialized view `TO` the alias used to capture the old schema of the target for
#   the view and fail with a `LOGICAL_ERROR` exception, as the target was loaded later with the new schema;
# - the `loop` table function took the old schema of the target and failed with `UNKNOWN_IDENTIFIER` for the new column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
MV_PATH="05457_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}/mv_target"
LOOP_PATH="05457_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}/loop_target"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mv_target (x Int32) ENGINE = IcebergS3(s3_conn, filename = '${MV_PATH}');
CREATE TABLE $DB.loop_target (x Int32) ENGINE = IcebergS3(s3_conn, filename = '${LOOP_PATH}');
CREATE TABLE alias_mv_target ENGINE = Alias($DB, mv_target);
CREATE TABLE alias_loop_target ENGINE = Alias($DB, loop_target);
CREATE TABLE src (x Int32) ENGINE = Null;
CREATE MATERIALIZED VIEW mv TO alias_mv_target AS SELECT * FROM src;
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO src VALUES (1)"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.loop_target VALUES (1)"

# Change the schema behind the back of the lazy database: tables outside of it over the same Iceberg paths add a column.
$CLICKHOUSE_CLIENT --query "
DETACH DATABASE $DB;
CREATE TABLE writer_mv_target ENGINE = IcebergS3(s3_conn, filename = '${MV_PATH}');
CREATE TABLE writer_loop_target ENGINE = IcebergS3(s3_conn, filename = '${LOOP_PATH}');
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE writer_mv_target ADD COLUMN y Nullable(Int32)"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE writer_loop_target ADD COLUMN y Nullable(Int32)"
$CLICKHOUSE_CLIENT --query "ALTER TABLE src ADD COLUMN y Nullable(Int32)"

# Attaching the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE $DB"

# The first access to the target of the view is an `INSERT` into the source of the view.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO src VALUES (2, 22)"
$CLICKHOUSE_CLIENT --query "SELECT * FROM writer_mv_target ORDER BY x"

# The first access to the other table is the `loop` table function over its alias.
$CLICKHOUSE_CLIENT --query "SELECT x, y FROM loop(alias_loop_target) LIMIT 1"

$CLICKHOUSE_CLIENT --query "
DROP TABLE mv; DROP TABLE src; DROP TABLE alias_mv_target; DROP TABLE alias_loop_target;
DROP TABLE writer_mv_target; DROP TABLE writer_loop_target; DROP DATABASE $DB SYNC;"
