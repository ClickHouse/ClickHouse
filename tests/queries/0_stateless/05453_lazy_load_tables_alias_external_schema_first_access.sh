#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires Iceberg (USE_AVRO build option) and S3

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in for the real storage, and an
# `Alias` or a materialized view refreshes the external metadata (e.g. the schema of an Iceberg table) only of a
# target that is already materialized, so that `DESCRIBE` does not load it. Here the external schema changes while
# the database is detached, and the first access after `ATTACH` goes through the wrappers:
# - a `SELECT` through an `Alias` used to resolve the columns from the `CREATE TABLE` query of the target and fail
#   with `UNKNOWN_IDENTIFIER` for the new column;
# - a mutation through an `Alias` of a materialized view materialized only the view, not its inner table, and failed
#   with a `LOGICAL_ERROR` exception, as the mutation ran against the old schema.

# Use `IcebergS3` rather than `IcebergLocal`, because the local object storage becomes read-only after
# `ATTACH`, which would block the mutation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
TABLE_PATH="05453_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}/t"
VIEW_PATH="05453_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}/mv"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.t (c0 String) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}');
CREATE TABLE $DB.src (c0 String) ENGINE = Null;
CREATE MATERIALIZED VIEW $DB.mv (c0 String) ENGINE = IcebergS3(s3_conn, filename = '${VIEW_PATH}') AS SELECT c0 FROM $DB.src;
CREATE TABLE alias_t ENGINE = Alias($DB, t);
CREATE TABLE alias_mv ENGINE = Alias($DB, mv);
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.t VALUES ('a')"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.src VALUES ('a')"

# Change the schema behind the back of the lazy database: tables outside of it over the same Iceberg paths add a column.
$CLICKHOUSE_CLIENT --query "
DETACH DATABASE $DB;
CREATE TABLE writer_t ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}');
CREATE TABLE writer_mv ENGINE = IcebergS3(s3_conn, filename = '${VIEW_PATH}');
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE writer_t ADD COLUMN c1 Nullable(Int32)"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE writer_mv ADD COLUMN c1 Nullable(Int32)"

# Attaching the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE $DB"

# The first access to the table is a `SELECT` of the new column through the alias.
$CLICKHOUSE_CLIENT --query "SELECT c0, c1 FROM alias_t"

# The first access to the inner table of the view is a mutation through the alias of the view.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE alias_mv UPDATE c0 = 'b' WHERE TRUE"
$CLICKHOUSE_CLIENT --query "SELECT c0 FROM alias_mv"
$CLICKHOUSE_CLIENT --query "SELECT * FROM writer_mv"

$CLICKHOUSE_CLIENT --query "DROP TABLE alias_t; DROP TABLE alias_mv; DROP TABLE writer_t; DROP TABLE writer_mv; DROP DATABASE $DB SYNC;"
