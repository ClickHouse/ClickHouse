#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/128455
# Tables created by 26.6 and older record a `last-column-id` that does not count nested field ids. ADD COLUMN must
# not reuse such an id: new ids start after the largest id of any schema (the current one and those of dropped columns).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_A="t_${CLICKHOUSE_DATABASE}_${RANDOM}_a"
TABLE_A_DIR="${USER_FILES_PATH}/${TABLE_A}"
TABLE_B="t_${CLICKHOUSE_DATABASE}_${RANDOM}_b"
TABLE_B_DIR="${USER_FILES_PATH}/${TABLE_B}"
TABLE_C="t_${CLICKHOUSE_DATABASE}_${RANDOM}_c"
TABLE_C_DIR="${USER_FILES_PATH}/${TABLE_C}"
TABLE_D="t_${CLICKHOUSE_DATABASE}_${RANDOM}_d"
TABLE_D_DIR="${USER_FILES_PATH}/${TABLE_D}"
rm -rf "${TABLE_A_DIR}" "${TABLE_B_DIR}" "${TABLE_C_DIR}" "${TABLE_D_DIR}"
cleanup() {
    rm -rf "${TABLE_A_DIR}" "${TABLE_B_DIR}" "${TABLE_C_DIR}" "${TABLE_D_DIR}"
    for table in ${TABLE_A} ${TABLE_B} ${TABLE_C} ${TABLE_D}; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}; DROP TABLE IF EXISTS ${table}_reopened"
    done
}
trap cleanup EXIT

# Publishes a new metadata version that differs from the latest one only by `last-column-id`, as 26.6 wrote it.
# A new file rather than an in-place edit, because the server caches parsed metadata files by path.
lower_last_column_id() {
    python3 - "$1/metadata" "$2" <<'PY'
import json, os, re, sys
metadata_dir, value = sys.argv[1], int(sys.argv[2])
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
metadata["last-column-id"] = value
metadata["last-updated-ms"] += 1
tmp = os.path.join(metadata_dir, ".tmp_next")
with open(tmp, "w") as f:
    json.dump(metadata, f)
os.rename(tmp, os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"))
PY
}

# Field ids of the current schema, nested ones included, and `last-column-id` of the latest metadata version.
field_ids() {
    python3 - "$1/metadata" <<'PY'
import json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
ids = []
def walk(node, prefix):
    if not isinstance(node, dict):
        return
    if node["type"] == "struct":
        for field in node["fields"]:
            ids.append((field["id"], prefix + field["name"]))
            walk(field["type"], prefix + field["name"] + ".")
    elif node["type"] == "list":
        ids.append((node["element-id"], prefix + "element"))
        walk(node["element"], prefix + "element.")
    elif node["type"] == "map":
        ids.append((node["key-id"], prefix + "key"))
        ids.append((node["value-id"], prefix + "value"))
        walk(node["key"], prefix + "key.")
        walk(node["value"], prefix + "value.")
walk(schema, "")
print(" ".join(f"{path}={field_id}" for field_id, path in sorted(ids)), f"last-column-id={metadata['last-column-id']}")
PY
}

# The reproducer from the issue: ids id=1 arr=2 arr.element=3, and 26.6 records last-column-id=2.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --multiquery -q "
CREATE TABLE ${TABLE_A} (id Int64, arr Array(Int64)) ENGINE = IcebergLocal('${TABLE_A_DIR}/');
INSERT INTO ${TABLE_A} VALUES (1, [1, 2]);
"
lower_last_column_id "${TABLE_A_DIR}" 2
# A second table over the same path starts from the lowered value, as a newer server opening an old table does.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --multiquery -q "
CREATE TABLE ${TABLE_A}_reopened ENGINE = IcebergLocal('${TABLE_A_DIR}/');
ALTER TABLE ${TABLE_A}_reopened ADD COLUMN extra Nullable(String);
SELECT * FROM ${TABLE_A}_reopened ORDER BY id;
INSERT INTO ${TABLE_A}_reopened VALUES (2, [3], 'x');
SELECT * FROM ${TABLE_A}_reopened ORDER BY id;
"
field_ids "${TABLE_A_DIR}"

# The same with a `Map`: ids id=1 m=2 m.key=3 m.value=4 m.value.element=5.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --multiquery -q "
CREATE TABLE ${TABLE_D} (id Int64, m Map(String, Array(Int64))) ENGINE = IcebergLocal('${TABLE_D_DIR}/');
INSERT INTO ${TABLE_D} VALUES (1, map('k', [1, 2]));
"
lower_last_column_id "${TABLE_D_DIR}" 2
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --multiquery -q "
CREATE TABLE ${TABLE_D}_reopened ENGINE = IcebergLocal('${TABLE_D_DIR}/');
ALTER TABLE ${TABLE_D}_reopened ADD COLUMN extra Nullable(String);
SELECT * FROM ${TABLE_D}_reopened ORDER BY id;
"
field_ids "${TABLE_D_DIR}"

# A dropped column keeps its ids 2, 3, 4 in the first schema and in the data file, so they must not be reused either.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --multiquery -q "
CREATE TABLE ${TABLE_B} (id Int64, t Tuple(a Int64, b Int64)) ENGINE = IcebergLocal('${TABLE_B_DIR}/');
INSERT INTO ${TABLE_B} VALUES (1, (10, 20));
ALTER TABLE ${TABLE_B} DROP COLUMN t;
"
lower_last_column_id "${TABLE_B_DIR}" 2
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --multiquery -q "
CREATE TABLE ${TABLE_B}_reopened ENGINE = IcebergLocal('${TABLE_B_DIR}/');
SELECT * FROM ${TABLE_B}_reopened ORDER BY id;
ALTER TABLE ${TABLE_B}_reopened ADD COLUMN c Nullable(Int64);
ALTER TABLE ${TABLE_B}_reopened ADD COLUMN d Nullable(Int64);
SELECT * FROM ${TABLE_B}_reopened ORDER BY id;
"
field_ids "${TABLE_B_DIR}"

# A top-level column named `a.b` (id 2) and the field `b` of the tuple `a` (id 3) have the same dotted path.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_C} (a Tuple(b Int64), \`a.b\` Int64) ENGINE = IcebergLocal('${TABLE_C_DIR}/')"
lower_last_column_id "${TABLE_C_DIR}" 2
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --multiquery -q "
CREATE TABLE ${TABLE_C}_reopened ENGINE = IcebergLocal('${TABLE_C_DIR}/');
ALTER TABLE ${TABLE_C}_reopened ADD COLUMN c Nullable(Int64);
"
field_ids "${TABLE_C_DIR}"
