#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: `IcebergLocal` needs the USE_AVRO build option.
# Tag no-replicated-database: `IcebergLocal` is not replicated.

# Data files written before an optional `variant` or `geometry` field was added to an Iceberg table read
# back with NULL in it. Adding a required field stays forbidden.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}"
cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version = 3"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

# ClickHouse cannot add `variant` or `geometry` fields, so append the schema that Spark's
# `ALTER TABLE ... ADD COLUMN v VARIANT` writes, once with `v` optional and once with `v` required.
read -r OPTIONAL_METADATA REQUIRED_METADATA <<< "$(python3 - "${TABLE_PATH}metadata" <<'EOF'
import copy, json, os, re, sys

metadata_dir = sys.argv[1]
version = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
    base = json.load(f)
last_id = base["last-column-id"]
current = next(s for s in base["schemas"] if s["schema-id"] == base["current-schema-id"])
next_schema_id = max(s["schema-id"] for s in base["schemas"]) + 1

def write(v_required, offset):
    metadata = copy.deepcopy(base)
    schema = copy.deepcopy(current)
    schema["schema-id"] = next_schema_id + offset
    schema["fields"] += [
        {"id": last_id + 1, "name": "v", "required": v_required, "type": "variant"},
        {"id": last_id + 2, "name": "g", "required": False, "type": "geometry"},
    ]
    metadata["schemas"].append(schema)
    metadata["current-schema-id"] = schema["schema-id"]
    metadata["last-column-id"] = last_id + 2
    metadata["last-updated-ms"] += 1 + offset
    name = f"v{version + 1 + offset}.metadata.json"
    with open(os.path.join(metadata_dir, name), "w") as f:
        json.dump(metadata, f)
    return f"metadata/{name}"

print(write(False, 0), write(True, 1))
EOF
)"

OPTIONAL="icebergLocal('${TABLE_PATH}', 'Parquet', SETTINGS iceberg_metadata_file_path = '${OPTIONAL_METADATA}')"
REQUIRED="icebergLocal('${TABLE_PATH}', 'Parquet', SETTINGS iceberg_metadata_file_path = '${REQUIRED_METADATA}')"

${CLICKHOUSE_CLIENT} --allow_geo_types_in_iceberg=1 --query "DESCRIBE ${OPTIONAL} SETTINGS describe_compact_output = 1"
${CLICKHOUSE_CLIENT} --allow_geo_types_in_iceberg=1 --query "SELECT id, v, g FROM ${OPTIONAL} ORDER BY id"
${CLICKHOUSE_CLIENT} --allow_geo_types_in_iceberg=1 --query "SELECT id FROM ${REQUIRED} ORDER BY id" 2>&1 \
    | grep -oF "with required values to the table during schema evolution" | head -1
