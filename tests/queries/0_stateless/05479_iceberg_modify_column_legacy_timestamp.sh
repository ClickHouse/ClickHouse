#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# ClickHouse declares `DateTime` and `DateTime64` columns as Iceberg `timestamptz`, but tables it wrote
# before that change have `timestamp` columns, and `timestamp` cannot be promoted to `timestamptz`.
# `ALTER TABLE ... MODIFY COLUMN` on such a column must keep the `timestamp` type instead of rejecting
# the ALTER as an invalid schema evolution, also when it only makes the column `Nullable`.
#
# Every step runs in a separate clickhouse-local process, so no step can see metadata cached by an
# earlier one. The `timestamp` column is produced by rewriting the metadata file before any data is
# written, so the manifest files never carry a conflicting `timestamptz` copy of the schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE_PATH="${TABLE_DIR}/t/"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_PATH}"
trap 'rm -rf "${TABLE_DIR}" 2>/dev/null' EXIT

LOCAL="${CLICKHOUSE_LOCAL} --session_timezone UTC --allow_insert_into_iceberg=1 --async_insert=0"

${LOCAL} --query "CREATE TABLE t (k Int64, ts DateTime64(6)) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

# Make the column a `timestamp`, as a table written by an older ClickHouse version has it.
sed -i 's/"timestamptz"/"timestamp"/g' "${TABLE_PATH}"metadata/*.metadata.json

${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t VALUES (1, '2024-01-02 03:04:05.123456');
"

echo "--- no-op MODIFY COLUMN ---"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    ALTER TABLE t MODIFY COLUMN ts DateTime64(6);
    SELECT 'ok';
"

echo "--- MODIFY COLUMN to Nullable ---"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    ALTER TABLE t MODIFY COLUMN ts Nullable(DateTime64(6));
    SELECT 'ok';
"

${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t VALUES (2, NULL);
"

echo "--- rows ---"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    SELECT k, ts, toTypeName(ts) FROM t ORDER BY k;
"

echo "--- Iceberg type of the column in the current schema ---"
LATEST_METADATA=$(ls "${TABLE_PATH}"metadata/v*.metadata.json | sort -V | tail -1)
python3 - "${LATEST_METADATA}" <<'PY'
import json, sys
meta = json.load(open(sys.argv[1]))
schema = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
field = next(f for f in schema["fields"] if f["name"] == "ts")
print(field["type"], "required" if field["required"] else "optional")
PY
