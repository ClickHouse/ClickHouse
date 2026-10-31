#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)

# Iceberg format version 1 requires the table metadata fields `schema` (the current schema) and
# `partition-spec` (the fields of the default partition spec). Every metadata file of a v1 table must
# carry them, in sync with `schemas` / `current-schema-id` and `partition-specs` / `default-spec-id`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PARTITIONED="tp_${CLICKHOUSE_DATABASE}"
TABLE_V2="t2_${CLICKHOUSE_DATABASE}"
TABLE_DIR="${USER_FILES_PATH}/${TABLE}"
TABLE_PARTITIONED_DIR="${USER_FILES_PATH}/${TABLE_PARTITIONED}"
TABLE_V2_DIR="${USER_FILES_PATH}/${TABLE_V2}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${TABLE};
        DROP TABLE IF EXISTS ${TABLE_PARTITIONED};
        DROP TABLE IF EXISTS ${TABLE_V2}"
    rm -rf "${TABLE_DIR}" "${TABLE_PARTITIONED_DIR}" "${TABLE_V2_DIR}"
}
cleanup
trap cleanup EXIT

query()
{
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "$1"
}

# Inspects the latest metadata file of the table.
check()
{
    local label=$1
    local table_dir=$2
    local latest
    latest=$(find "${table_dir}/metadata" -name 'v*.metadata.json' | sort -V | tail -1)
    python3 - "${label}" "${latest}" <<'EOF'
import json, sys
label, path = sys.argv[1], sys.argv[2]
m = json.load(open(path))
current = [s for s in m.get("schemas", []) if s.get("schema-id") == m.get("current-schema-id")]
default = [s for s in m.get("partition-specs", []) if s.get("spec-id") == m.get("default-spec-id")]
schema = m.get("schema")
spec = m.get("partition-spec")
print(label,
      "has_schema=%d" % ("schema" in m),
      "has_partition_spec=%d" % ("partition-spec" in m),
      "schema_in_sync=%d" % (schema is not None and len(current) == 1 and schema == current[0]),
      "spec_in_sync=%d" % (spec is not None and len(default) == 1 and spec == default[0].get("fields")),
      "schema_id=%s" % (schema.get("schema-id") if schema else "none"),
      "fields=%s" % (",".join(f["name"] for f in schema["fields"]) if schema else "none"),
      "spec_fields=%s" % (len(spec) if spec is not None else "none"))
EOF
}

query "CREATE TABLE ${TABLE} (id Int32, s String, n Nullable(Int32)) ENGINE = IcebergLocal('${TABLE_DIR}/') SETTINGS iceberg_format_version = 1"
check create "${TABLE_DIR}"
query "INSERT INTO ${TABLE} VALUES (1, 'a', 10)"
check insert "${TABLE_DIR}"
query "ALTER TABLE ${TABLE} ADD COLUMN x Nullable(Int64)"
check add_column "${TABLE_DIR}"
query "ALTER TABLE ${TABLE} MODIFY COLUMN n Nullable(Int64)"
check modify_column "${TABLE_DIR}"
query "ALTER TABLE ${TABLE} RENAME COLUMN s TO s2"
check rename_column "${TABLE_DIR}"
query "ALTER TABLE ${TABLE} DROP COLUMN x"
check drop_column "${TABLE_DIR}"
query "INSERT INTO ${TABLE} VALUES (2, 'b', 20)"
check insert_after_alter "${TABLE_DIR}"
query "SELECT * FROM ${TABLE} ORDER BY id"

query "CREATE TABLE ${TABLE_PARTITIONED} (id Int32, p Int32) ENGINE = IcebergLocal('${TABLE_PARTITIONED_DIR}/') PARTITION BY p SETTINGS iceberg_format_version = 1"
check partitioned_create "${TABLE_PARTITIONED_DIR}"
query "INSERT INTO ${TABLE_PARTITIONED} VALUES (1, 100), (2, 200)"
check partitioned_insert "${TABLE_PARTITIONED_DIR}"
query "SELECT * FROM ${TABLE_PARTITIONED} ORDER BY id"

# Format version 2 metadata does not carry the version 1 fields.
query "CREATE TABLE ${TABLE_V2} (id Int32) ENGINE = IcebergLocal('${TABLE_V2_DIR}/') SETTINGS iceberg_format_version = 2"
check v2_create "${TABLE_V2_DIR}"
