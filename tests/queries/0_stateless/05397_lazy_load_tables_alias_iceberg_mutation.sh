#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires Iceberg (USE_AVRO build option) and S3

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in for the real storage.
# `ALTER ... UPDATE` through an `Alias` refreshes the external metadata of the target through the alias, and
# `mutate` of an Iceberg table executes the mutation against that state without refreshing it again. The
# refresh used to stop at the stand-in, so the mutation ran against the state the table was loaded with.

# Use `IcebergS3` rather than `IcebergLocal`, because the local object storage becomes read-only after
# `ATTACH`, which would block the mutation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
TABLE_PATH="05397_lazy_iceberg/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.t (c0 String) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}');
CREATE TABLE alias_t ENGINE = Alias($DB, t);
"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "INSERT INTO $DB.t VALUES ('a')"

# Reloading the database installs the stand-in, as a server restart does.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"

# The first access to the table is the mutation through the alias.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE alias_t UPDATE c0 = 'b' WHERE TRUE"
$CLICKHOUSE_CLIENT --query "SELECT c0 FROM alias_t"

# An external schema change after the table has been loaded: a second table over the same Iceberg path, outside
# of the lazy database, adds a column behind the back of the loaded storage. The mutation through the alias used
# to run against the old schema and fail with a `LOGICAL_ERROR` exception, and a `SELECT` of the table kept
# reading the state it was loaded with.
$CLICKHOUSE_CLIENT --query "CREATE TABLE writer ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE writer ADD COLUMN c1 Nullable(Int32)"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --query "ALTER TABLE alias_t UPDATE c0 = 'c' WHERE TRUE"
$CLICKHOUSE_CLIENT --query "SELECT * FROM alias_t"
$CLICKHOUSE_CLIENT --query "SELECT * FROM $DB.t"

$CLICKHOUSE_CLIENT --query "DROP TABLE alias_t; DROP TABLE writer; DROP DATABASE $DB SYNC;"
