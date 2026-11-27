#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# The fields of a struct are read by Iceberg field id, so names that match several fields only case-insensitively are not
# ambiguous there: fields `AB` and `ab` renamed to `Ab` and `aB` keep their values, as a whole tuple, as subcolumns and in a filter.
# A field `Ab` added next to `AB` and `ab`, in a struct, in a list or as a column, is not in the file and reads as NULL.

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
    CREATE TABLE added (id Int32, AB Int32, ab Int32, t Tuple(AB Int32, ab Int32), l Array(Tuple(AB Int32, ab Int32)))
        ENGINE = IcebergLocal('${TMP_DIR}/added/', 'ORC');
    INSERT INTO added VALUES (1, 11, 22, (11, 22), [(11, 22)]);
"

# Renames or adds the fields in a new current schema, the way another engine changes them, and binds the snapshot to it.
evolve()
{
python3 - "$1/metadata" "$2" <<'PY'
import copy, json, os, re, sys
metadata_dir, mode = sys.argv[1], sys.argv[2]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = copy.deepcopy(metadata["schemas"][-1])
fields = {field["name"]: field for field in schema["fields"]}
if mode == "rename":
    for element in fields["t"]["type"]["fields"]:
        element["name"] = {"AB": "Ab", "ab": "aB"}[element["name"]]
else:
    for parent in (fields["t"]["type"]["fields"], fields["l"]["type"]["element"]["fields"], schema["fields"]):
        metadata["last-column-id"] += 1
        parent.append({"id": metadata["last-column-id"], "name": "Ab", "required": False, "type": "int"})
schema["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
metadata["schemas"].append(schema)
metadata["current-schema-id"] = schema["schema-id"]
metadata["last-updated-ms"] += 1
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = schema["schema-id"]
with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY
}
evolve "${TMP_DIR}/t" rename
evolve "${TMP_DIR}/added" add

${CLICKHOUSE_LOCAL} -q "
    SET input_format_orc_case_insensitive_column_matching = 1;
    SELECT * FROM icebergLocal('${TMP_DIR}/t/');
    SELECT t.Ab, t.aB FROM icebergLocal('${TMP_DIR}/t/');
    SELECT id FROM icebergLocal('${TMP_DIR}/t/') WHERE t.Ab = 11;
    SELECT id FROM icebergLocal('${TMP_DIR}/t/') WHERE t.aB = 11;
"

${CLICKHOUSE_LOCAL} -q "
    SET input_format_orc_case_insensitive_column_matching = 1, input_format_orc_filter_push_down = 1;
    SELECT * FROM icebergLocal('${TMP_DIR}/added/');
    SELECT t.AB, t.Ab, l.Ab, AB, Ab FROM icebergLocal('${TMP_DIR}/added/');
    SELECT id FROM icebergLocal('${TMP_DIR}/added/') WHERE isNull(t.Ab) AND isNull(Ab);
"
