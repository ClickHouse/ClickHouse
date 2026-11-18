#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# The fields of a struct are read by Iceberg field id, so names that match several fields only case-insensitively are not
# ambiguous there: fields `AB` and `ab` renamed to `Ab` and `aB` keep their values, as a whole tuple, as subcolumns and in a filter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

${CLICKHOUSE_LOCAL} -q "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t (id Int32, t Tuple(AB Int32, ab Int32)) ENGINE = IcebergLocal('${TMP_DIR}/t/', 'ORC');
    INSERT INTO t VALUES (1, (11, 22));
"

# Renames the fields in a new current schema, the way another engine renames them, and binds the snapshot to it.
python3 - "${TMP_DIR}/t/metadata" <<'PY'
import copy, json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = copy.deepcopy(metadata["schemas"][-1])
for field in schema["fields"]:
    if field["name"] == "t":
        for element in field["type"]["fields"]:
            element["name"] = {"AB": "Ab", "ab": "aB"}[element["name"]]
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
    SET input_format_orc_case_insensitive_column_matching = 1;
    SELECT * FROM icebergLocal('${TMP_DIR}/t/');
    SELECT t.Ab, t.aB FROM icebergLocal('${TMP_DIR}/t/');
    SELECT id FROM icebergLocal('${TMP_DIR}/t/') WHERE t.Ab = 11;
    SELECT id FROM icebergLocal('${TMP_DIR}/t/') WHERE t.aB = 11;
"
