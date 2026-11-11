#!/usr/bin/env bash
# Tags: no-fasttest
# INSERT into an Iceberg table applies the declared DEFAULTs and CHECK constraints: https://github.com/ClickHouse/ClickHouse/issues/130538

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

T="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
T2="${T}_other"
TABLE_PATH="${USER_FILES_PATH}/${T}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${T}_s3; DROP TABLE IF EXISTS ${T}_mv; DROP TABLE IF EXISTS ${T}_src; DROP TABLE IF EXISTS ${T2}; DROP TABLE IF EXISTS ${T}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"

echo "insert"
${CLIENT} --query "
    CREATE TABLE ${T} (k Int32, s String DEFAULT 'x' COMMENT 'sc', e Int64 DEFAULT k * 100, CONSTRAINT k_positive CHECK k > 0)
    ENGINE = IcebergLocal('${TABLE_PATH}') COMMENT 'tc'"
${CLIENT} --query "INSERT INTO ${T} (k) VALUES (1)"
${CLIENT} --query "INSERT INTO ${T} (k) VALUES (-1)" 2>&1 | grep -o -m1 VIOLATED_CONSTRAINT
echo '{"k": 2}' | ${CLIENT} --query "INSERT INTO ${T} (k) FORMAT JSONEachRow"
${CLIENT} --query "SELECT * FROM ${T} ORDER BY k"

echo "definition"
${CLIENT} --query "SELECT name, default_kind, default_expression, comment FROM system.columns WHERE database = currentDatabase() AND table = '${T}' ORDER BY position"
${CLIENT} --query "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = '${T}'"

echo "prewhere"
${CLIENT} --query "SELECT count() FROM ${T} PREWHERE s = 'x'"

echo "constraint is not used by reads"
${CLIENT} --query "CREATE TABLE ${T2} ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} --query "INSERT INTO ${T2} (k, s, e) VALUES (-5, 'y', 0)"
${CLIENT} --query "SELECT count() FROM ${T} WHERE k <= 0 SETTINGS optimize_using_constraints = 1, convert_query_to_cnf = 1"

echo "alter"
${CLIENT} --query "ALTER TABLE ${T} ADD COLUMN c Nullable(Int32) DEFAULT 42"
${CLIENT} --query "INSERT INTO ${T} (k) VALUES (3)"
${CLIENT} --query "ALTER TABLE ${T} DELETE WHERE k = 2"
${CLIENT} --query "SELECT k, c FROM ${T} ORDER BY k"

echo "older snapshot"
S1=$(${CLIENT} --query "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${T}' ORDER BY made_current_at LIMIT 1")
${CLIENT} --query "SELECT count() FROM ${T} SETTINGS iceberg_snapshot_id = ${S1}"
${CLIENT} --query "ALTER TABLE ${T} MODIFY SETTING iceberg_metadata_async_prefetch_period_ms = 1000"
${CLIENT} --query "ALTER TABLE ${T} ADD COLUMN f Nullable(Int64) DEFAULT c + 1"
${CLIENT} --query "SELECT count() FROM ${T} SETTINGS iceberg_snapshot_id = ${S1}"
${CLIENT} --query "ALTER TABLE ${T} DROP COLUMN c" 2>&1 | grep -o -m1 ILLEGAL_COLUMN
${CLIENT} --iceberg_snapshot_id="${S1}" --query "ALTER TABLE ${T} ADD COLUMN g Nullable(Int32)" 2>&1 | grep -o -m1 NOT_IMPLEMENTED
${CLIENT} --iceberg_snapshot_id="${S1}" --query "ALTER TABLE ${T} MODIFY SETTING iceberg_metadata_async_prefetch_period_ms = 2000" 2>&1 | grep -o -m1 NOT_IMPLEMENTED
${CLIENT} --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = '${T}' AND name = 'g'"
${CLIENT} --query "DETACH TABLE ${T}"
${CLIENT} --query "ATTACH TABLE ${T}"
${CLIENT} --query "INSERT INTO ${T} (k) VALUES (4)"
${CLIENT} --query "INSERT INTO ${T} (k) VALUES (-4)" 2>&1 | grep -o -m1 VIOLATED_CONSTRAINT
${CLIENT} --query "SELECT * FROM ${T} WHERE k = 4"

echo "materialized view"
${CLIENT} --query "CREATE TABLE ${T}_src (k Int32) ENGINE = Memory"
${CLIENT} --query "CREATE MATERIALIZED VIEW ${T}_mv TO ${T} AS SELECT k FROM ${T}_src"
${CLIENT} --query "INSERT INTO ${T}_src VALUES (6)"
${CLIENT} --query "SELECT k, s, e FROM ${T} WHERE k = 6"

echo "schema changed by another writer"
${CLIENT} --query "ALTER TABLE ${T2} RENAME COLUMN s TO s2"
${CLIENT} --query "INSERT INTO ${T} (k) VALUES (7)"
${CLIENT} --query "SELECT k, s2 FROM ${T} WHERE k = 7"

echo "comment of a table that is not a data lake"
${CLIENT} --query "CREATE TABLE ${T}_s3 (id UInt64) ENGINE = S3('http://localhost:1/no-such-bucket/*.parquet', 'test', 'testtest', 'Parquet')"
${CLIENT} --query "ALTER TABLE ${T}_s3 MODIFY COMMENT 'updated'"
${CLIENT} --query "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = '${T}_s3'"
