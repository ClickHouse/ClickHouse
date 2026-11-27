#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# With input_format_parquet_case_insensitive_column_matching, a field `Ab` added to an Iceberg table next to `AB` is still
# another field: an older data file has no value for it, so it reads as NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

${CLICKHOUSE_LOCAL} -q "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t (id Int32, AB Int32) ENGINE = IcebergLocal('${TMP_DIR}/t/', 'Parquet');
    INSERT INTO t VALUES (1, 11);
"

# Adds `Ab` in a new current schema, the way another engine adds it, and binds the snapshot to it.
python3 - "${TMP_DIR}/t/metadata" <<'PY'
import copy, json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = copy.deepcopy(metadata["schemas"][-1])
metadata["last-column-id"] += 1
schema["fields"].append({"id": metadata["last-column-id"], "name": "Ab", "required": False, "type": "int"})
schema["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
metadata["schemas"].append(schema)
metadata["current-schema-id"] = schema["schema-id"]
metadata["last-updated-ms"] += 1
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = schema["schema-id"]
with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY

${CLICKHOUSE_LOCAL} -q "
    SELECT AB, Ab FROM icebergLocal('${TMP_DIR}/t/') SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
"
