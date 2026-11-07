#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires Iceberg (USE_AVRO build option) and S3

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in for the real storage, and an
# `Alias` refreshes the external metadata (e.g. the schema of an Iceberg table) only of a target that is already
# materialized, so that `DESCRIBE` does not load it. Here the external schema changes while the database is detached,
# and the first access after `ATTACH` goes through aliases:
# - an `INSERT` (synchronous and asynchronous) through an `Alias` took the insertion schema from the `CREATE TABLE`
#   query of the target and failed with `NO_SUCH_COLUMN_IN_TABLE` for the new column;
# - schema inference of `merge` through two successive aliases materialized only the first target, missed the new
#   column and the `SELECT` failed with `UNKNOWN_IDENTIFIER`.

# Use `IcebergS3` rather than `IcebergLocal`, because the local object storage becomes read-only after `ATTACH`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
BASE_PATH="05454_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.t_sync (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${BASE_PATH}/t_sync');
CREATE TABLE $DB.t_async (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${BASE_PATH}/t_async');
CREATE TABLE $DB.t_merge (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${BASE_PATH}/t_merge');
CREATE TABLE alias_sync ENGINE = Alias($DB, t_sync);
CREATE TABLE alias_async ENGINE = Alias($DB, t_async);
-- An alias cannot be created over another alias, so the second hop replaces an ordinary table.
CREATE TABLE chain_b (c0 Int32) ENGINE = Memory;
CREATE TABLE chain_a ENGINE = Alias(chain_b);
DROP TABLE chain_b;
CREATE TABLE chain_b ENGINE = Alias($DB, t_merge);
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.t_sync VALUES (1)"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.t_async VALUES (1)"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.t_merge VALUES (1)"

# Change the schema behind the back of the lazy database: tables outside of it over the same Iceberg paths add a column.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB"
for name in t_sync t_async t_merge
do
    $CLICKHOUSE_CLIENT --query "CREATE TABLE writer_$name ENGINE = IcebergS3(s3_conn, filename = '${BASE_PATH}/$name')"
    $CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE writer_$name ADD COLUMN c1 Nullable(Int32)"
done

# Attaching the database installs the stand-ins, as a server restart does.
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE $DB"

# The first access to the table is an `INSERT` of the new column through the alias.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO alias_sync (c0, c1) VALUES (2, 2)"
$CLICKHOUSE_CLIENT --query "SELECT c0, c1 FROM writer_t_sync ORDER BY c0"

$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --async_insert=1 --wait_for_async_insert=1 --query "INSERT INTO alias_async (c0, c1) VALUES (2, 2)"
$CLICKHOUSE_CLIENT --query "SELECT c0, c1 FROM writer_t_async ORDER BY c0"

# The first access to the table is a `SELECT` of the new column through `merge` over successive aliases.
$CLICKHOUSE_CLIENT --query "SELECT c0, c1 FROM merge(currentDatabase(), '^chain_a$')"

$CLICKHOUSE_CLIENT --query "
DROP TABLE alias_sync; DROP TABLE alias_async; DROP TABLE chain_a; DROP TABLE chain_b;
DROP TABLE writer_t_sync; DROP TABLE writer_t_async; DROP TABLE writer_t_merge;
DROP DATABASE $DB SYNC;
"
