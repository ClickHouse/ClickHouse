#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.
# Tag no-replicated-database: IcebergLocal is not replicated.

# An Iceberg schema that violates the specification is rejected with ICEBERG_SPECIFICATION_VIOLATION naming the field:
# an `identifier-field-ids` entry that is not a required primitive field outside lists, maps and optional structs,
# a list or a map without the id of its element, key or value, a geo type with invalid parameters,
# a table schema that is not a `struct`. Every schema of `schemas` is checked, not only the current one.
# A rejected metadata.json is rejected again when it is cached; older metadata versions are not checked.
# An ALTER that would produce such a schema is refused with BAD_ARGUMENTS.
# The `schema_core__*` and `types_*` cases are from apache/iceberg-verification
# (Apache-2.0, commit 12b050929377853252123e345fa0659452da4a73, `table-spec/{schema,types}/*/cases.json`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_DIR="${USER_FILES_PATH}/${TABLE}"
rm -rf "$DIR" "$TABLE_DIR" "${TABLE_DIR}_cached"
mkdir -p "$DIR"
cleanup() {
    rm -rf "$DIR" "$TABLE_DIR" "${TABLE_DIR}_cached"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; DROP TABLE IF EXISTS ${TABLE}_reopened"
}
trap cleanup EXIT

python3 - "$DIR" "${TABLE_DIR}_cached" <<'PY'
import json
import os
import sys

root = sys.argv[1]


def table(schemas, current_schema_id=0, version=2):
    t = {
        "format-version": version,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "",
        "last-sequence-number": 0,
        "last-updated-ms": 1750772174828,
        "last-column-id": 1000,
        "current-schema-id": current_schema_id,
        "schemas": schemas,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "current-snapshot-id": -1,
        "refs": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
    }
    return t


def schema(fields, **extra):
    return dict({"type": "struct", "schema-id": 0, "fields": fields}, **extra)


def column(type_):
    return schema([{"id": 1000, "name": "c", "required": False, "type": type_}])


ID = {"id": 1, "name": "id", "required": True, "type": "long"}

cases = {
    "schema_core__reject-identifier-field-does-not-exist": schema([ID], **{"identifier-field-ids": [999]}),
    "schema_core__reject-identifier-optional-field": schema(
        [ID, {"id": 2, "name": "note", "required": False, "type": "string"}], **{"identifier-field-ids": [2]}),
    "schema_core__reject-identifier-double-field": schema(
        [ID, {"id": 2, "name": "score", "required": True, "type": "double"}], **{"identifier-field-ids": [2]}),
    "schema_core__reject-identifier-float-field": schema(
        [ID, {"id": 2, "name": "score", "required": True, "type": "float"}], **{"identifier-field-ids": [2]}),
    "schema_core__reject-identifier-non-primitive-field": schema(
        [ID, {"id": 2, "name": "loc", "required": True,
              "type": {"type": "struct", "fields": [{"id": 3, "name": "x", "required": True, "type": "long"}]}}],
        **{"identifier-field-ids": [2]}),
    "schema_core__reject-identifier-nested-in-list": schema(
        [ID, {"id": 2, "name": "rows", "required": True,
              "type": {"type": "list", "element-id": 3, "element-required": True,
                       "element": {"type": "struct", "fields": [{"id": 4, "name": "k", "required": True, "type": "string"}]}}}],
        **{"identifier-field-ids": [4]}),
    "schema_core__reject-identifier-nested-in-map-value": schema(
        [ID, {"id": 2, "name": "rows", "required": True,
              "type": {"type": "map", "key-id": 3, "key": "string", "value-id": 4, "value-required": True,
                       "value": {"type": "struct", "fields": [{"id": 5, "name": "k", "required": True, "type": "string"}]}}}],
        **{"identifier-field-ids": [5]}),
    "schema_core__reject-identifier-map-key": schema(
        [ID, {"id": 2, "name": "props", "required": True,
              "type": {"type": "map", "key-id": 3, "key": "string", "value-id": 4, "value-required": True, "value": "string"}}],
        **{"identifier-field-ids": [3]}),
    "schema_core__reject-identifier-nested-in-optional-struct": schema(
        [ID, {"id": 2, "name": "loc", "required": False,
              "type": {"type": "struct", "fields": [{"id": 3, "name": "x", "required": True, "type": "long"}]}}],
        **{"identifier-field-ids": [3]}),
    "schema_core__identifier-nested-in-required-struct": schema(
        [ID, {"id": 2, "name": "key", "required": True,
              "type": {"type": "struct", "fields": [{"id": 3, "name": "part", "required": True, "type": "string"}]}}],
        **{"identifier-field-ids": [3]}),
    "schema_core__schema-id-and-identifier-fields": dict(schema(
        [{"id": 1, "name": "tenant", "required": True, "type": "string"},
         {"id": 2, "name": "key", "required": True, "type": "long"},
         {"id": 3, "name": "payload", "required": False, "type": "string"}],
        **{"identifier-field-ids": [1, 2]}), **{"schema-id": 7}),
    "types_nested__list-missing-element-id": column({"type": "list", "element-required": True, "element": "string"}),
    "types_nested__map-missing-key-id": column(
        {"type": "map", "key": "string", "value-id": 4, "value-required": False, "value": "double"}),
    "types_nested__map-missing-value-id": column(
        {"type": "map", "key-id": 3, "key": "string", "value-required": False, "value": "double"}),
    "types_nested__list-optional-element": column({"type": "list", "element-id": 2, "element-required": False, "element": "long"}),
    "types_nested__map-string-double": column(
        {"type": "map", "key-id": 3, "key": "string", "value-id": 4, "value-required": False, "value": "double"}),
    "types_geospatial__geography-unknown-algorithm": column("geography(OGC:CRS84, bogus)"),
    "geography_empty_crs": column("geography(, spherical)"),
    "geometry_empty_crs": column("geometry()"),
    "geography_algorithm_case": column("geography(OGC:CRS84, Karney)"),
    "geography_crs_only": column("geography(OGC:CRS84)"),
    "geometry_srid": column("geometry(srid:4326)"),
    "schema_without_type": {k: v for k, v in column("long").items() if k != "type"},
    "schema_of_type_list": dict(column("long"), type="list"),
    "historical_schema_invalid": table(
        [schema([ID], **{"identifier-field-ids": [999]}), dict(schema([ID]), **{"schema-id": 1})], current_schema_id=1),
    "historical_schema_valid": table([schema([ID]), dict(schema([ID]), **{"schema-id": 1})], current_schema_id=1),
    "historical_schema_invalid_geo": table(
        [column("geography(OGC:CRS84, bogus)"), dict(column("long"), **{"schema-id": 1})], current_schema_id=1),
    # Only `schemas` is read when it is present.
    "v2_legacy_schema_not_checked": dict(table([schema([ID])]), schema={"schema-id": 0, "fields": [ID]}),
}

# Format version 1 with only the legacy `schema`.
for name, legacy in (("v1_schema_without_type", {"schema-id": 0, "fields": [ID]}), ("v1_schema", schema([ID]))):
    t = table([])
    del t["schemas"], t["current-schema-id"], t["table-uuid"], t["last-sequence-number"], t["last-partition-id"]
    t.update({"format-version": 1, "schema": legacy, "partition-spec": []})
    cases[name] = t


def write(location, metadata, version=1):
    metadata["location"] = location
    os.makedirs(os.path.join(location, "metadata"), exist_ok=True)
    with open(os.path.join(location, "metadata", f"v{version}.metadata.json"), "w") as f:
        json.dump(metadata, f)


for name, metadata in cases.items():
    if "format-version" not in metadata:
        metadata = table([metadata], current_schema_id=metadata["schema-id"])
    write(os.path.join(root, name), metadata)

# An invalid version followed by a valid one.
write(os.path.join(root, "old_version_invalid"), table([schema([ID], **{"identifier-field-ids": [999]})]))
newer = table([schema([ID])])
newer["last-updated-ms"] += 1
write(os.path.join(root, "old_version_invalid"), newer, version=2)

write(sys.argv[2], table([schema([ID], **{"identifier-field-ids": [999]})]))
PY

# Describes a table, or prints the error code and `token` when the message contains it. `$3` are more arguments of icebergLocal.
function describe()
{
    local out
    out=$($CLICKHOUSE_LOCAL --allow_geo_types_in_iceberg 1 --query "DESCRIBE icebergLocal('$DIR/$1'${3:+, $3}) SETTINGS describe_compact_output = 1" 2>&1)
    if [[ "$out" == *"DB::Exception"* ]]
    then
        echo "$1 $(grep -oE '\([A-Z_]+\)' <<< "$out" | tail -1) $(grep -oF -- "$2" <<< "$out" | head -1)"
    else
        echo "$1"
        echo "$out"
    fi
}

echo "--- identifier fields"
describe schema_core__reject-identifier-field-does-not-exist "999"
describe schema_core__reject-identifier-optional-field "'note'"
describe schema_core__reject-identifier-double-field "'score'"
describe schema_core__reject-identifier-float-field "'score'"
describe schema_core__reject-identifier-non-primitive-field "'loc'"
describe schema_core__reject-identifier-nested-in-list "'rows.element.k'"
describe schema_core__reject-identifier-nested-in-map-value "'rows.value.k'"
describe schema_core__reject-identifier-map-key "'props.key'"
describe schema_core__reject-identifier-nested-in-optional-struct "'loc.x'"
describe schema_core__identifier-nested-in-required-struct
describe schema_core__schema-id-and-identifier-fields

echo "--- list and map ids"
describe types_nested__list-missing-element-id "'element-id'"
describe types_nested__map-missing-key-id "'key-id'"
describe types_nested__map-missing-value-id "'value-id'"
describe types_nested__list-optional-element
describe types_nested__map-string-double

echo "--- geo types"
describe types_geospatial__geography-unknown-algorithm "'bogus'"
describe geography_empty_crs "'geography(,spherical)'"
describe geometry_empty_crs "'geometry()'"
describe geography_algorithm_case
describe geography_crs_only
describe geometry_srid

echo "--- schema type"
describe schema_without_type "'type'"
describe schema_of_type_list "'list'"
describe v1_schema_without_type "'type'"
describe v1_schema
describe v2_legacy_schema_not_checked

echo "--- every schema of schemas"
describe historical_schema_invalid "999"
describe historical_schema_valid
describe historical_schema_invalid_geo "'bogus'"
# Choosing the latest version by `last-updated-ms` reads every version.
describe old_version_invalid "" "SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 1"

echo "--- alter"
# A table whose current schema has the identifier fields `id` and `k.part`, as another engine could write it.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --multiquery -q "
CREATE TABLE ${TABLE} (id Int64, k Tuple(part Int64), v Nullable(Int64), w Int64) ENGINE = IcebergLocal('${TABLE_DIR}/');
INSERT INTO ${TABLE} VALUES (1, tuple(10), 100, 1000);
"
# Prints the identifier fields of the current schema of the latest metadata version; with an argument,
# publishes a new version whose current schema has the identifier fields at these paths.
identifiers() {
    python3 - "${TABLE_DIR}/metadata" "$@" <<'PY'
import json, os, re, sys
metadata_dir, paths = sys.argv[1], sys.argv[2:]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
ids = {}
def walk(fields, prefix):
    for field in fields:
        ids[prefix + field["name"]] = field["id"]
        if isinstance(field["type"], dict) and field["type"]["type"] == "struct":
            walk(field["type"]["fields"], prefix + field["name"] + ".")
walk(schema["fields"], "")
if not paths:
    names = {field_id: path for path, field_id in ids.items()}
    print("identifier fields:", " ".join(names[i] for i in schema.get("identifier-field-ids", [])))
    sys.exit(0)
schema["identifier-field-ids"] = [ids[path] for path in paths]
metadata["last-updated-ms"] += 1
tmp = os.path.join(metadata_dir, ".tmp_next")
with open(tmp, "w") as f:
    json.dump(metadata, f)
os.rename(tmp, os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"))
PY
}
identifiers id k.part
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --multiquery -q "
CREATE TABLE ${TABLE}_reopened ENGINE = IcebergLocal('${TABLE_DIR}/');
DESCRIBE ${TABLE}_reopened SETTINGS describe_compact_output = 1;
"
for alter in "DROP COLUMN id" "DROP COLUMN k" "MODIFY COLUMN id Nullable(Int64)"
do
    out=$(${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 -q "ALTER TABLE ${TABLE}_reopened ${alter}" 2>&1)
    echo "$alter $(grep -oE '\([A-Z_]+\)' <<< "$out" | tail -1) $(grep -oF 'its new schema would be invalid' <<< "$out" | head -1)"
done
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --multiquery -q "
ALTER TABLE ${TABLE}_reopened DROP COLUMN v;
SELECT * FROM ${TABLE}_reopened;
DESCRIBE icebergLocal('${TABLE_DIR}/') SETTINGS describe_compact_output = 1;
"
identifiers

echo "--- cached"
for _ in 1 2
do
    out=$(${CLICKHOUSE_CLIENT} -q "DESCRIBE icebergLocal('${TABLE_DIR}_cached/') SETTINGS use_iceberg_metadata_files_cache = 1" 2>&1)
    echo "cached $(grep -oE '\([A-Z_]+\)' <<< "$out" | tail -1) $(grep -oF -- 999 <<< "$out" | head -1)"
done
