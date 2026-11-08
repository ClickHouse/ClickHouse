#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# An Iceberg field added with an `initial-default` reads as that default in the data files written before it was added,
# both when such a file is read under its own older schema and when it is read under the schema that has the field.
# Tables written by other engines: `data_iceberg/impala` (Apache Impala test data at e35eb8f3582449d736e74ce32f1138f5f5dc085b,
# Apache-2.0) and `data_iceberg/duckdb` (DuckDB Iceberg extension tests at b57f45c2c78965c71f699aa23ae73c292923668e, MIT).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

LOCAL="${CLICKHOUSE_LOCAL} --session_timezone UTC"

# Binds every snapshot to the current schema, in place: this is how a reader sees a data file written under an older
# schema after its manifest was rewritten under the newer schema and its snapshot expired.
bind_to_current_schema()
{
    python3 - "$1/metadata" <<'PY'
import json, os, re, sys
metadata_dir = sys.argv[1]
latest = max((f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")), key=lambda f: int(re.match(r"v?(\d+)", f).group(1)))
with open(os.path.join(metadata_dir, latest)) as f:
    metadata = json.load(f)
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = metadata["current-schema-id"]
with open(os.path.join(metadata_dir, latest), "w") as f:
    json.dump(metadata, f)
PY
}

# Adds the next metadata version with the current schema 1: schema 0 with fields added the way another engine adds them,
# because ClickHouse cannot add a field with a default, a required field or a field of a struct.
add_fields()
{
    python3 - "$1/metadata" "$2" <<'PY'
import copy, json, os, re, sys
metadata_dir, mode = sys.argv[1], sys.argv[2]
latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
    metadata = json.load(f)
schema = copy.deepcopy(metadata["schemas"][0])
last_id = metadata["last-column-id"]
def new_id():
    global last_id
    last_id += 1
    return last_id
fields = schema["fields"]
if mode == "readd":
    fields[:] = [f for f in fields if f["name"] != "j"]
    fields.append({"id": new_id(), "name": "j", "required": False, "type": "int", "initial-default": 9, "write-default": 9})
    struct = next(f for f in fields if f["name"] == "t")["type"]
    struct["fields"] = [{"id": new_id(), "name": "j", "required": False, "type": "int", "initial-default": 9, "write-default": 9}]
elif mode == "bad_long":
    fields.append({"id": new_id(), "name": "d", "required": False, "type": "long", "initial-default": "abc"})
elif mode == "bad_struct":
    x = new_id()
    fields.append({"id": new_id(), "name": "s", "required": False, "initial-default": {str(x): 1},
        "type": {"type": "struct", "fields": [{"id": x, "name": "x", "required": False, "type": "int"}]}})
else:
    by_name = {f["name"]: f for f in fields}
    by_name["t"]["type"]["fields"].append({"id": new_id(), "name": "y", "required": False, "type": "int", "initial-default": 5})
    by_name["l"]["type"]["element"]["fields"].append({"id": new_id(), "name": "y", "required": False, "type": "int", "initial-default": 5})
    fields.append({"id": new_id(), "name": "d", "required": False, "type": "long", "initial-default": 7})
    fields.append({"id": new_id(), "name": "r", "required": True, "type": "long", "initial-default": 8, "write-default": 8})
    x = new_id()
    fields.append({"id": new_id(), "name": "s", "required": False, "initial-default": {},
        "type": {"type": "struct", "fields": [{"id": x, "name": "x", "required": False, "type": "int", "initial-default": 6}]}})
    x = new_id()
    fields.append({"id": new_id(), "name": "n", "required": False,
        "type": {"type": "struct", "fields": [{"id": x, "name": "x", "required": False, "type": "int", "initial-default": 6}]}})
    element = new_id()
    fields.append({"id": new_id(), "name": "ll", "required": False, "initial-default": [1, None],
        "type": {"type": "list", "element-id": element, "element": "int", "element-required": False}})
schema["schema-id"] = 1
metadata["schemas"].append(schema)
metadata["current-schema-id"] = 1
metadata["last-column-id"] = last_id
metadata["last-updated-ms"] += 1
with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY
}

# Copies the latest metadata file of the table at $1 to $2 without the `initial-default` keys.
copy_without_defaults()
{
    python3 - "$1/metadata" "$2" <<'PY'
import json, os, re, sys
metadata_dir, name = sys.argv[1], sys.argv[2]
latest = max((f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")), key=lambda f: int(re.match(r"v?(\d+)", f).group(1)))
with open(os.path.join(metadata_dir, latest)) as f:
    metadata = json.load(f)
def strip(node):
    if isinstance(node, dict):
        node.pop("initial-default", None)
        for value in node.values():
            strip(value)
    elif isinstance(node, list):
        for value in node:
            strip(value)
strip(metadata["schemas"])
with open(os.path.join(metadata_dir, name), "w") as f:
    json.dump(metadata, f)
PY
}

# The schema ids under which the data files of the table at $1 are read.
schema_ids()
{
    ${LOCAL} -q "
        CREATE TABLE schema_ids ENGINE = IcebergLocal('$1');
        SELECT 'schema ids', arraySort(groupArray(schema_id)) FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'schema_ids';
    "
}

impala_queries()
{
    local dir=$1
    echo "${dir}"
    schema_ids "${dir}/iceberg_v3_default_value"
    ${LOCAL} -q "
        SELECT * FROM icebergLocal('${dir}/iceberg_v3_default_value') ORDER BY i;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value') WHERE j = -1 SETTINGS optimize_move_to_prewhere = 1;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value') WHERE j = -1 SETTINGS optimize_move_to_prewhere = 0;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value') WHERE j IS NULL;
        SELECT min(j), max(j) FROM icebergLocal('${dir}/iceberg_v3_default_value');
        SELECT * FROM icebergLocal('${dir}/iceberg_v3_default_value_orc') ORDER BY i;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value_orc') WHERE j = -1;
        SELECT * FROM icebergLocal('${dir}/iceberg_v3_default_value_avro') ORDER BY i;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value_avro') WHERE j = -1;
    "
}

duckdb_queries()
{
    ${LOCAL} -q "
        SELECT col1, col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_time, col_timestamp,
            col_timestamptz, col_string, col_uuid, hex(col_fixed), hex(col_binary)
        FROM icebergLocal('data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults') ORDER BY col1;
        SELECT x.col1, x.col_boolean, x.col_integer, x.col_long, x.col_float, x.col_double, x.col_decimal, x.col_date, x.col_time,
            x.col_timestamp, x.col_timestamptz, x.col_string, x.col_uuid, hex(x.col_fixed), hex(x.col_binary)
        FROM (SELECT a AS x FROM icebergLocal('data/persistent/add_columns_with_defaults_in_struct/default.db/add_columns_with_defaults_in_struct'))
        ORDER BY x.col_boolean;
    "
}

# Tables of other engines, as written: the older data files are read under their own schema.
cd "${CUR_DIR}/data_iceberg/impala" || exit 1
impala_queries iceberg_v3
${LOCAL} -q "
    SELECT 'all_types';
    SELECT id, col_boolean, col_int, col_bigint, col_float, col_double, col_decimal, col_string, col_date, col_timestamp,
        hex(col_binary), col_timestamp_epoch, col_timestamp_iso
    FROM icebergLocal('iceberg_v3/iceberg_v3_all_types') ORDER BY id;
    SELECT 'complex_default';
    SELECT i, j, ts_tz FROM icebergLocal('iceberg_v3/test_complex_default') ORDER BY i;
    SELECT 'default_part';
    SELECT * FROM icebergLocal('iceberg_v3/test_default_part') ORDER BY i;
    SELECT i FROM icebergLocal('iceberg_v3/test_default_part') WHERE j = -1 ORDER BY i;
    SELECT i FROM icebergLocal('iceberg_v3/test_default_part') WHERE j = 5;
    SELECT j, count() FROM icebergLocal('iceberg_v3/test_default_part') GROUP BY j ORDER BY j;
    SELECT 'time travel';
    SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_default_value') SETTINGS iceberg_snapshot_id = 7752776210164022855;
"
cd "${CUR_DIR}/data_iceberg/duckdb" || exit 1
echo 'duckdb'
duckdb_queries

# The same tables with the older data files read under the current schema. A local path must be inside the current
# directory, so the copies are referred to by relative paths.
cd "${TMP_DIR}" || exit 1
mkdir -p impala
for table in iceberg_v3_default_value iceberg_v3_default_value_orc iceberg_v3_default_value_avro; do
    cp -rp "${CUR_DIR}/data_iceberg/impala/iceberg_v3/${table}" impala/
    bind_to_current_schema "impala/${table}"
done
impala_queries impala
for table in add_columns_with_defaults add_columns_with_defaults_in_struct; do
    mkdir -p "duckdb/data/persistent/${table}"
    cp -rp "${CUR_DIR}/data_iceberg/duckdb/data/persistent/${table}/default.db" "duckdb/data/persistent/${table}/"
    bind_to_current_schema "duckdb/data/persistent/${table}/default.db/${table}"
done
cd duckdb || exit 1
echo 'duckdb, current schema'
schema_ids data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults
duckdb_queries

# The defaults are a part of the identity of the cached data: the same files read through metadata without them first.
table="data/persistent/add_columns_with_defaults_in_struct/default.db/add_columns_with_defaults_in_struct"
metadata=$(basename "$(ls "${table}"/metadata/00003-*.metadata.json)")
copy_without_defaults "${table}" 00004-no-defaults.metadata.json
${LOCAL} -q "
    SET use_columns_cache = 1;
    SELECT 'columns cache', x.col1, x.col_integer
    FROM (SELECT a AS x FROM icebergLocal('${table}', SETTINGS iceberg_metadata_file_path = 'metadata/00004-no-defaults.metadata.json')) ORDER BY ALL;
    SELECT 'columns cache', x.col1, x.col_integer
    FROM (SELECT a AS x FROM icebergLocal('${table}', SETTINGS iceberg_metadata_file_path = 'metadata/${metadata}')) ORDER BY ALL;
"
cd "${TMP_DIR}" || exit 1
copy_without_defaults impala/iceberg_v3_default_value v5.metadata.json
for metadata in v5 v4; do
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'query condition cache, ${metadata}', i
        FROM icebergLocal('${TMP_DIR}/impala/iceberg_v3_default_value', SETTINGS iceberg_metadata_file_path = 'metadata/${metadata}.metadata.json')
        WHERE j = -1 SETTINGS use_query_condition_cache = 1, optimize_move_to_prewhere = 1;
    "
done

# Tables written by ClickHouse, evolved by another engine: defaults of nested fields, of a required field, of structs and of
# a list with a NULL element.
for format in Parquet ORC Avro; do
    table="${format}"
    ${LOCAL} --allow_insert_into_iceberg=1 -q "
        CREATE TABLE t (a Int32, t Tuple(x Nullable(Int32)), l Array(Tuple(x Nullable(Int32)))) ENGINE = IcebergLocal('${table}/', '${format}');
        INSERT INTO t VALUES (1, (10), [(11)]);
    "
    add_fields "${table}" nested
    echo "${format}"
    schema_ids "${table}/"
    ${LOCAL} -q "
        SELECT a, t, l, d, s, n, ll FROM icebergLocal('${table}/');
        SELECT a, r FROM icebergLocal('${table}/');
    "
    bind_to_current_schema "${table}"
    echo "${format}, current schema"
    schema_ids "${table}/"
    if [ "${format}" = Avro ]; then
        # A field missing inside an Avro record is not supported.
        ${LOCAL} -q "
            SELECT a, d, r, s, n, ll FROM icebergLocal('${table}/');
            SELECT t FROM icebergLocal('${table}/'); -- { serverError INCORRECT_DATA }
        "
    else
        ${LOCAL} -q "
            SELECT a, t, l, d, r, s, n, ll FROM icebergLocal('${table}/');
            SELECT a FROM icebergLocal('${table}/') WHERE d = 7 SETTINGS optimize_move_to_prewhere = 1;
            SELECT a FROM icebergLocal('${table}/') WHERE d = 7 SETTINGS optimize_move_to_prewhere = 0;
        "
    fi

    # The dropped fields `j` and the added fields `j` have different field ids.
    table="${format}_readd"
    ${LOCAL} --allow_insert_into_iceberg=1 -q "
        CREATE TABLE t (a Int32, j Nullable(Int32), t Tuple(j Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
        INSERT INTO t VALUES (1, 100, (100));
    "
    add_fields "${table}" readd
    ${LOCAL} -q "SELECT 'readd', * FROM icebergLocal('${table}/')"
    bind_to_current_schema "${table}"
    # A field of an Avro record is still matched by name.
    columns=$([ "${format}" = Avro ] && echo 'a, j' || echo '*')
    ${LOCAL} -q "
        SELECT 'readd, current schema', ${columns} FROM icebergLocal('${table}/');
        SELECT 'readd, current schema', ${columns} FROM icebergLocal('${table}/') WHERE j = 9;
    "
done

# Malformed defaults.
for mode in bad_long bad_struct; do
    ${LOCAL} --allow_insert_into_iceberg=1 -q "
        CREATE TABLE t (a Int32, t Tuple(x Nullable(Int32)), l Array(Tuple(x Nullable(Int32)))) ENGINE = IcebergLocal('${mode}/');
        INSERT INTO t VALUES (1, (10), [(11)]);
    "
    add_fields "${mode}" "${mode}"
    ${LOCAL} -q "SELECT * FROM icebergLocal('${mode}/')" 2>&1 | grep -o -m1 'Code: [0-9]*'
done
