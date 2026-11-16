#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The Iceberg v3 type `unknown` is read as `Nullable(Nothing)`: its values are always NULL.
# `data_iceberg/unknown_type` was written by Spark 4.1.2 with Iceberg 1.11.0, which stores `void` as `unknown`:
#   CREATE TABLE t (id bigint, u void) USING iceberg TBLPROPERTIES ('format-version' = '3');
#   INSERT INTO t VALUES (1, NULL), (2, NULL);
#   ALTER TABLE t ADD COLUMN later void;
#   ALTER TABLE t ADD COLUMN s struct<v: void, x: int>;
#   INSERT INTO t VALUES (3, NULL, NULL, named_struct('v', NULL, 'x', 3)); -- once per format: Parquet, ORC, Avro
# Parquet and ORC files omit these columns, Avro files store them as `null`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="$CUR_DIR/data_iceberg/unknown_type"
TABLES="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TABLES"
mkdir -p "$TABLES"
trap 'rm -rf "$TABLES"' EXIT

echo "--- table written by Spark"
$CLICKHOUSE_LOCAL --query "DESCRIBE icebergLocal('$T') SETTINGS describe_compact_output = 1"
$CLICKHOUSE_LOCAL --query "SELECT id, u, later, s, _file LIKE '%.avro' FROM icebergLocal('$T') ORDER BY id"
$CLICKHOUSE_LOCAL --query "SELECT id, u, later, s.v FROM icebergLocal('$T') WHERE coalesce(u, later, s.v, id) > 2 ORDER BY id SETTINGS optimize_move_to_prewhere = 1"
# Reading `later` in a join makes the planner read its manifest statistics.
$CLICKHOUSE_LOCAL --query "SELECT a.id, a.later FROM icebergLocal('$T') AS a INNER JOIN icebergLocal('$T') AS b ON a.id = b.id ORDER BY a.id SETTINGS use_datalake_metadata_statistics = 1, use_statistics = 1"

echo "--- schemas without data"
python3 - "$TABLES" <<'EOF'
import json, os, sys

def column(t):
    return [{"id": 1, "name": "c", "required": False, "type": t}]

cases = {
    "optional": [{"id": 1, "name": "id", "required": True, "type": "long"}, {"id": 2, "name": "future", "required": False, "type": "unknown"}],
    "list_element": column({"type": "list", "element-id": 2, "element": "unknown", "element-required": False}),
    "map_value": column({"type": "map", "key-id": 2, "key": "string", "value-id": 3, "value": "unknown", "value-required": False}),
    "required": [{"id": 1, "name": "id", "required": True, "type": "long"}, {"id": 2, "name": "future", "required": True, "type": "unknown"}],
    "required_struct_field": column({"type": "struct", "fields": [{"id": 2, "name": "v", "required": True, "type": "unknown"}]}),
    "required_list_element": column({"type": "list", "element-id": 2, "element": "unknown", "element-required": True}),
    "map_key": column({"type": "map", "key-id": 2, "key": "unknown", "value-id": 3, "value": "string", "value-required": False}),
}
for name, fields in cases.items():
    os.makedirs(os.path.join(sys.argv[1], name, "metadata"))
    metadata = {
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "file:///unused",
        "last-sequence-number": 0,
        "last-updated-ms": 1,
        "last-column-id": 1000,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": fields}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
    }
    with open(os.path.join(sys.argv[1], name, "metadata", "v1.metadata.json"), "w") as f:
        json.dump(metadata, f)
EOF
for name in optional list_element map_value required required_struct_field required_list_element map_key
do
    echo "$name"
    # A rejected schema prints only the error code.
    if ! $CLICKHOUSE_LOCAL --query "DESCRIBE icebergLocal('$TABLES/$name') SETTINGS describe_compact_output = 1" 2>"$TABLES/stderr"
    then
        grep -o -m1 -E '\([A-Z_]+\)$' "$TABLES/stderr" | tr -d '()'
    fi
done

# The spec allows to promote `unknown` to any primitive type (Iceberg Java 1.11 refuses it). Schema 3 promotes `u` to `int` and renames it.
echo "--- unknown promoted to int"
cp -r "$T" "$TABLES/promoted"
python3 - "$TABLES/promoted/metadata" <<'EOF'
import copy, json, os, sys

with open(os.path.join(sys.argv[1], "v9.metadata.json")) as f:
    metadata = json.load(f)
schema = copy.deepcopy(next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"]))
schema["schema-id"] = 3
for field in schema["fields"]:
    if field["id"] == 2:
        field["name"], field["type"] = "u_int", "int"
metadata["schemas"].append(schema)
metadata["current-schema-id"] = 3
with open(os.path.join(sys.argv[1], "v10.metadata.json"), "w") as f:
    json.dump(metadata, f)
EOF
$CLICKHOUSE_LOCAL --query "SELECT id, u_int, toTypeName(u_int) FROM icebergLocal('$TABLES/promoted', SETTINGS iceberg_metadata_file_path = 'metadata/v10.metadata.json') ORDER BY id"
