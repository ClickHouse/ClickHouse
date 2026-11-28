#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# The top-level columns of an Iceberg ORC file are read by field id, as Parquet reads them: a renamed column, two swapped
# columns and a column renamed to the name of a dropped one keep their values, as whole columns, as subcolumns and in filters.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

${CLICKHOUSE_LOCAL} -q "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE r (id Int32, t Tuple(AB Int32, ab Int32), l Array(Tuple(x Int32)), a Int32, b Int32, c Int32, d Int32)
        ENGINE = IcebergLocal('${TMP_DIR}/r/', 'ORC');
    INSERT INTO r VALUES (1, (11, 22), [(7), (8)], 100, 200, 300, 400);
    CREATE TABLE ci (id Int32, a Nullable(Int32)) ENGINE = IcebergLocal('${TMP_DIR}/ci/', 'ORC');
    INSERT INTO ci VALUES (1, 100);
"

# Changes the columns in a new current schema, the way another engine changes them, and binds the snapshot to it.
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
    fields["t"]["name"], fields["l"]["name"] = "u", "m"
    fields["a"]["name"], fields["b"]["name"] = "b", "a"
    schema["fields"].remove(fields["c"])
    fields["d"]["name"] = "c"
else:
    fields["a"]["name"] = "A"
    metadata["last-column-id"] += 1
    schema["fields"].append({"id": metadata["last-column-id"], "name": "a", "required": False, "type": "int"})
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
evolve "${TMP_DIR}/r" rename
evolve "${TMP_DIR}/ci" ci

${CLICKHOUSE_LOCAL} -q "
    SELECT * FROM icebergLocal('${TMP_DIR}/r/', 'ORC');
    SELECT * FROM icebergLocal('${TMP_DIR}/r/');
    SELECT u.AB, u.ab, m.x FROM icebergLocal('${TMP_DIR}/r/', 'ORC');
"

${CLICKHOUSE_LOCAL} -q "
    SET input_format_orc_filter_push_down = 1;
    SELECT count() FROM icebergLocal('${TMP_DIR}/r/', 'ORC') WHERE b = 100;
    SELECT count() FROM icebergLocal('${TMP_DIR}/r/', 'ORC') WHERE a = 200;
    SELECT count() FROM icebergLocal('${TMP_DIR}/r/', 'ORC') WHERE c = 400;
    SELECT count() FROM icebergLocal('${TMP_DIR}/r/', 'ORC') WHERE u.AB = 11;
"

# A column renamed to a case variant of a new column's name: the new column is not in the file.
${CLICKHOUSE_LOCAL} -q "
    SELECT a, A FROM icebergLocal('${TMP_DIR}/ci/', 'ORC') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
"
