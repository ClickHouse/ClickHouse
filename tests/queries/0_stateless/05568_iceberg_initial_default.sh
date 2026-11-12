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

# Binds every snapshot of the tables at $@ to the current schema, in place: this is how a reader sees a data file written under
# an older schema after its manifest was rewritten under the newer schema and its snapshot expired.
bind_to_current_schema()
{
    python3 - "$@" <<'PY'
import json, os, re, sys
for table in sys.argv[1:]:
    metadata_dir = os.path.join(table, "metadata")
    latest = max((f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")), key=lambda f: int(re.match(r"v?(\d+)", f).group(1)))
    with open(os.path.join(metadata_dir, latest)) as f:
        metadata = json.load(f)
    for snapshot in metadata["snapshots"]:
        snapshot["schema-id"] = metadata["current-schema-id"]
    with open(os.path.join(metadata_dir, latest), "w") as f:
        json.dump(metadata, f)
PY
}

# For each pair of a table and a mode in $@, adds the next metadata version with the current schema 1: schema 0 with fields added
# the way another engine adds them, because ClickHouse cannot add a field with a default, a required field or a field of a struct.
add_fields()
{
    python3 - "$@" <<'PY'
import copy, json, os, re, sys
for table, mode in zip(sys.argv[1::2], sys.argv[2::2]):
    metadata_dir = os.path.join(table, "metadata")
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
    elif mode == "readd_nested":
        struct = next(f for f in fields if f["name"] == "t")["type"]["fields"][0]["type"]
        struct["fields"] = [f for f in struct["fields"] if f["name"] != "j"]
        struct["fields"].append({"id": new_id(), "name": "j", "required": False, "type": "int", "initial-default": 9, "write-default": 9})
    elif mode == "rename":
        struct = next(f for f in fields if f["name"] == "t")["type"]
        renamed = next(f for f in struct["fields"] if f["name"] == "j")
        renamed["name"] = "y"
        renamed["initial-default"] = 9
    elif mode == "rename_to_dropped":
        struct = next(f for f in fields if f["name"] == "t")["type"]
        struct["fields"] = [f for f in struct["fields"] if f["name"] != "x"]
        struct["fields"][0]["name"] = "x"
    elif mode == "element_field":
        element = new_id()
        struct = next(f for f in fields if f["name"] == "t")["type"]
        struct["fields"].append({"id": new_id(), "name": "s", "required": False, "initial-default": {},
            "type": {"type": "struct", "fields": [{"id": new_id(), "name": "element", "required": False, "initial-default": [9],
                "type": {"type": "list", "element-id": element, "element": "int", "element-required": False}}]}})
    elif mode == "readd_in_list":
        by_name = {f["name"]: f for f in fields}
        u = by_name["u"]["type"]["fields"][1]["type"]["element"]
        for struct in (by_name["t"]["type"]["fields"][0]["type"]["element"], by_name["l"]["type"]["element"]["fields"][0]["type"], u):
            struct["fields"] = [f for f in struct["fields"] if f["name"] != "j"]
            struct["fields"].append({"id": new_id(), "name": "j", "required": False, "type": "int", "initial-default": 9})
    elif mode == "rename_list":
        by_name = {f["name"]: f for f in next(f for f in fields if f["name"] == "t")["type"]["fields"]}
        by_name["z"]["name"] = "k"
        by_name["s"]["name"] = "z"
        by_name["s"]["type"]["element"]["fields"] = [{"id": new_id(), "name": "j", "required": False, "type": "int", "initial-default": 9}]
    elif mode == "struct_in_struct":
        def xy():
            return {"type": "struct", "fields": [{"id": new_id(), "name": "x", "required": False, "type": "int", "initial-default": 6},
                {"id": new_id(), "name": "y", "required": False, "type": "int", "initial-default": 9}]}
        struct = next(f for f in fields if f["name"] == "t")["type"]
        struct["fields"].append({"id": new_id(), "name": "s", "required": False, "initial-default": {}, "type": xy()})
        element = new_id()
        element_type = xy()
        x, y = (f["id"] for f in element_type["fields"])
        struct["fields"].append({"id": new_id(), "name": "l", "required": False, "initial-default": [{str(x): 1, str(y): 2}, {str(x): 3}],
            "type": {"type": "list", "element-id": element, "element": element_type, "element-required": False}})
    elif mode == "bad_long":
        fields.append({"id": new_id(), "name": "d", "required": False, "type": "long", "initial-default": "abc"})
    elif mode == "bad_timestamp":
        fields.append({"id": new_id(), "name": "ts", "required": False, "type": "timestamp", "initial-default": "2017-11-16T22:31:08+01:00"})
    elif mode == "bad_timestamptz":
        fields.append({"id": new_id(), "name": "ts", "required": False, "type": "timestamptz", "initial-default": "2017-11-16T22:31:08"})
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

# The queries that print the schema ids under which the data files of the table at $1 are read, through the table named $2.
schema_ids()
{
    echo "
        CREATE TABLE $2 ENGINE = IcebergLocal('$1');
        SELECT 'schema ids', arraySort(groupArray(schema_id)) FROM system.iceberg_files WHERE database = currentDatabase() AND table = '$2';"
}

# Each test case runs in few processes: one `clickhouse-local` is much slower to start than a query.
impala_queries()
{
    local dir=$1
    ${LOCAL} -q "
        SELECT '${dir}';
        $(schema_ids "${dir}/iceberg_v3_default_value" schema_ids)
        SELECT * FROM icebergLocal('${dir}/iceberg_v3_default_value') ORDER BY i;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value') WHERE j = -1 SETTINGS optimize_move_to_prewhere = 1;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value') WHERE j = -1 SETTINGS optimize_move_to_prewhere = 0;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value') WHERE j IS NULL;
        SELECT min(j), max(j) FROM icebergLocal('${dir}/iceberg_v3_default_value');
        SELECT * FROM icebergLocal('${dir}/iceberg_v3_default_value_orc') ORDER BY i;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value_orc') WHERE j = -1;
        SELECT * FROM icebergLocal('${dir}/iceberg_v3_default_value_avro') ORDER BY i;
        SELECT i FROM icebergLocal('${dir}/iceberg_v3_default_value_avro') WHERE j = -1;
        $2
    "
}

duckdb_queries()
{
    ${LOCAL} -q "
        $1
        SELECT col1, col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_time, col_timestamp,
            col_timestamptz, col_string, col_uuid, hex(col_fixed), hex(col_binary)
        FROM icebergLocal('data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults') ORDER BY col1;
        SELECT x.col1, x.col_boolean, x.col_integer, x.col_long, x.col_float, x.col_double, x.col_decimal, x.col_date, x.col_time,
            x.col_timestamp, x.col_timestamptz, x.col_string, x.col_uuid, hex(x.col_fixed), hex(x.col_binary)
        FROM (SELECT a AS x FROM icebergLocal('data/persistent/add_columns_with_defaults_in_struct/default.db/add_columns_with_defaults_in_struct'))
        ORDER BY x.col_boolean;
        $2
    "
}

# Tables of other engines, as written: the older data files are read under their own schema.
cd "${CUR_DIR}/data_iceberg/impala" || exit 1
impala_queries iceberg_v3 "
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
duckdb_queries "SELECT 'duckdb';"

# The same tables with the older data files read under the current schema. A local path must be inside the current
# directory, so the copies are referred to by relative paths.
cd "${TMP_DIR}" || exit 1
mkdir -p impala
for table in iceberg_v3_default_value iceberg_v3_default_value_orc iceberg_v3_default_value_avro; do
    cp -rp "${CUR_DIR}/data_iceberg/impala/iceberg_v3/${table}" impala/
done
bind_to_current_schema impala/iceberg_v3_default_value impala/iceberg_v3_default_value_orc impala/iceberg_v3_default_value_avro
impala_queries impala
for table in add_columns_with_defaults add_columns_with_defaults_in_struct; do
    mkdir -p "duckdb/data/persistent/${table}"
    cp -rp "${CUR_DIR}/data_iceberg/duckdb/data/persistent/${table}/default.db" "duckdb/data/persistent/${table}/"
done
bind_to_current_schema duckdb/data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults \
    duckdb/data/persistent/add_columns_with_defaults_in_struct/default.db/add_columns_with_defaults_in_struct
cd duckdb || exit 1
# An identity partition value of the file, even NULL, comes before the default. The copy renames the metadata file:
# `final.metadata.json` is not read as a metadata version (#129988).
mixed=data/persistent/partition_values_missing_edge_cases/default/mixed
mkdir -p "$(dirname "${mixed}")"
cp -rp "${CUR_DIR}/data_iceberg/duckdb/${mixed}" "$(dirname "${mixed}")/"
mv "${mixed}/metadata/final.metadata.json" "${mixed}/metadata/v1.metadata.json"
MIXED="icebergLocal('${mixed}', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
duckdb_queries "
    SELECT 'duckdb, current schema';
    $(schema_ids data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults schema_ids)
" "
    SELECT 'identity partition values';
    SELECT id, region, address FROM ${MIXED} WHERE id IN (4, 9, 11) ORDER BY id;
    SELECT count(), sum(id) FROM ${MIXED} WHERE region IS NULL SETTINGS optimize_move_to_prewhere = 1;
    SELECT count(), sum(id) FROM ${MIXED} WHERE region IS NULL SETTINGS optimize_move_to_prewhere = 0;
    SELECT id FROM ${MIXED} WHERE region = 'unknown-region' AND address.country = 'unknown-country' ORDER BY id;
"

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
# a list with a NULL element. All tables of a format are written by one process, and their queries run in two more: before
# and after the files are bound to the current schema.
for format in Parquet ORC Avro; do
    create=""
    evolve=()
    bind=()
    before=""
    after=""

    table="${format}"
    create+="
        CREATE TABLE t_nested (a Int32, t Tuple(x Nullable(Int32)), l Array(Tuple(x Nullable(Int32)))) ENGINE = IcebergLocal('${table}/', '${format}');
        INSERT INTO t_nested VALUES (1, (10), [(11)]);"
    evolve+=("${table}" nested)
    bind+=("${table}")
    before+="
        SELECT '${format}';
        $(schema_ids "${table}/" schema_ids_before)
        SELECT a, t, l, d, s, n, ll FROM icebergLocal('${table}/');
        SELECT a, r FROM icebergLocal('${table}/');"
    after+="
        SELECT '${format}, current schema';
        $(schema_ids "${table}/" schema_ids_after)"
    if [ "${format}" = Avro ]; then
        # A field missing inside an Avro record is not supported.
        after+="
            SELECT a, d, r, s, n, ll FROM icebergLocal('${table}/');
            SELECT t FROM icebergLocal('${table}/'); -- { serverError INCORRECT_DATA }"
    else
        after+="
            SELECT a, t, l, d, r, s, n, ll FROM icebergLocal('${table}/');
            SELECT t.x, t.y, s.x, n.x FROM icebergLocal('${table}/');
            SELECT a FROM icebergLocal('${table}/') WHERE d = 7 SETTINGS optimize_move_to_prewhere = 1;
            SELECT a FROM icebergLocal('${table}/') WHERE d = 7 SETTINGS optimize_move_to_prewhere = 0;"
        if [ "${format}" = Parquet ]; then
            # A field with a default is not a missing column.
            after+="
                SELECT a, t, l, d, r, s, ll FROM icebergLocal('${table}/') SETTINGS input_format_parquet_allow_missing_columns = 0;"
        fi
    fi

    # The dropped fields `j` and the added fields `j` have different field ids.
    table="${format}_readd"
    create+="
        CREATE TABLE t_readd (a Int32, j Nullable(Int32), t Tuple(j Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
        INSERT INTO t_readd VALUES (1, 100, (100));"
    evolve+=("${table}" readd)
    bind+=("${table}")
    # The filter on `t.j` is not pushed down to the dropped field `t.j`.
    nested_filter="SELECT 't.j = 9', t FROM icebergLocal('${table}/') WHERE t.j = 9 SETTINGS optimize_move_to_prewhere = 0"
    before+="
        SELECT 'readd', * FROM icebergLocal('${table}/');
        ${nested_filter}, input_format_orc_filter_push_down = 1;
        ${nested_filter}, input_format_orc_filter_push_down = 0;"
    # A field of an Avro record is still matched by name.
    columns=$([ "${format}" = Avro ] && echo 'a, j' || echo '*')
    after+="
        SELECT 'readd, current schema', ${columns} FROM icebergLocal('${table}/');
        SELECT 'readd, current schema', ${columns} FROM icebergLocal('${table}/') WHERE j = 9;"
    if [ "${format}" != Avro ]; then
        after+="
            ${nested_filter}, input_format_orc_filter_push_down = 1;
            ${nested_filter}, input_format_orc_filter_push_down = 0;"
    fi

    # A projected struct is read by field id too: the re-added `t.s.j` is not the dropped one.
    if [ "${format}" != Avro ]; then
        table="${format}_readd_nested"
        create+="
            CREATE TABLE t_readd_nested (t Tuple(s Tuple(x Nullable(Int32), j Nullable(Int32)))) ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_readd_nested VALUES (((10, 100)));"
        evolve+=("${table}" readd_nested)
        bind+=("${table}")
        after+="
            SELECT 'readd nested', t FROM icebergLocal('${table}/');
            SELECT 'readd nested', tupleElement(t, 's') FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = 1;
            SELECT 'readd nested', tupleElement(t, 's') FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = 0;
            SELECT 'readd nested', t.s.x, t.s.j FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = 1;"
    fi

    # A renamed field with a default keeps its field id, so its stored value is read, not the default.
    if [ "${format}" != Avro ]; then
        table="${format}_rename"
        create+="
            CREATE TABLE t_rename (t Tuple(x Nullable(Int32), j Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_rename VALUES ((10, 100));"
        evolve+=("${table}" rename)
        bind+=("${table}")
        after+="
            SELECT 'rename', t FROM icebergLocal('${table}/');
            SELECT 'rename', t.x, t.y FROM icebergLocal('${table}/');
            SELECT 'rename', t.y FROM icebergLocal('${table}/');
            SELECT 'rename', t.x, t.y FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = 0;"
    fi

    # A field renamed to the name of a dropped field: the filter on it is not pushed down to the dropped field.
    if [ "${format}" != Avro ]; then
        table="${format}_rename_to_dropped"
        create+="
            CREATE TABLE t_rename_to_dropped (t Tuple(x Nullable(Int32), j Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_rename_to_dropped VALUES ((10, 100));"
        evolve+=("${table}" rename_to_dropped)
        bind+=("${table}")
        nested_filter="FROM icebergLocal('${table}/') WHERE t.x = 100 SETTINGS optimize_move_to_prewhere = 0, optimize_functions_to_subcolumns = 1"
        after+="
            SELECT 'rename to dropped', t, t.x FROM icebergLocal('${table}/');
            SELECT 'rename to dropped, t.x = 100', t ${nested_filter}, input_format_orc_filter_push_down = 1;
            SELECT 'rename to dropped, t.x = 100', t ${nested_filter}, input_format_orc_filter_push_down = 0;
            SELECT 'rename to dropped, t.x = 10', t FROM icebergLocal('${table}/') WHERE t.x = 10 SETTINGS input_format_orc_filter_push_down = 1;"
    fi

    # An added struct with a field named `element`, absent from the file, is not a list.
    if [ "${format}" != Avro ]; then
        table="${format}_element_field"
        create+="
            CREATE TABLE t_element_field (t Tuple(x Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_element_field VALUES ((10));"
        evolve+=("${table}" element_field)
        bind+=("${table}")
        after+="
            SELECT 'element field', t FROM icebergLocal('${table}/');
            SELECT 'element field', t.s.element FROM icebergLocal('${table}/');"
    fi

    # The same below a list, read through the column that holds it; `u.element` is a field, not the element of a list.
    if [ "${format}" = ORC ]; then
        table="${format}_readd_in_list"
        create+="
            CREATE TABLE t_readd_in_list (t Tuple(s Array(Tuple(x Nullable(Int32), j Nullable(Int32)))),
                l Array(Tuple(s Tuple(x Nullable(Int32), j Nullable(Int32)))),
                u Tuple(element Nullable(Int32), a Array(Tuple(x Nullable(Int32), j Nullable(Int32)))))
            ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_readd_in_list VALUES (([(10, 100)]), [((10, 100))], (1, [(10, 100)]));"
        evolve+=("${table}" readd_in_list)
        bind+=("${table}")
        after+="
            SELECT 'readd in list', t, l, u FROM icebergLocal('${table}/');
            SELECT 'readd in list', t.s.j, l.s.j, u.a.j FROM icebergLocal('${table}/');"
    fi

    # A list renamed to the old name of a renamed field is matched by field id also when that field is read too.
    if [ "${format}" = ORC ]; then
        table="${format}_rename_list"
        create+="
            CREATE TABLE t_rename_list (t Tuple(s Array(Tuple(j Nullable(Int32))), z Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_rename_list VALUES (([(100)], 7));"
        evolve+=("${table}" rename_list)
        bind+=("${table}")
        after+="
            SELECT 'rename list', t FROM icebergLocal('${table}/');
            SELECT 'rename list', t.z.j, t.k FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = 1;
            SELECT 'rename list', t.z.j, t.k FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = 0;"
    fi

    # A struct and a list of structs added to a struct: a read of some fields of the struct takes them from its default by name.
    if [ "${format}" != Avro ]; then
        table="${format}_struct_in_struct"
        create+="
            CREATE TABLE t_struct_in_struct (t Tuple(k Nullable(Int32))) ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_struct_in_struct VALUES ((10));"
        evolve+=("${table}" struct_in_struct)
        bind+=("${table}")
        for optimize in 1 0; do
            after+="
                SELECT 'struct in struct', ${optimize}, t FROM icebergLocal('${table}/');
                SELECT 'struct in struct', ${optimize}, t.s.y FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = ${optimize};
                SELECT 'struct in struct', ${optimize}, t.k, t.s.x FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = ${optimize};
                SELECT 'struct in struct', ${optimize}, count() FROM icebergLocal('${table}/') WHERE t.s.y = 9 SETTINGS optimize_functions_to_subcolumns = ${optimize};"
        done
    fi

    # A top-level field whose name has a dot is not a field of the struct named by its prefix, and neither is an element of a
    # struct whose name has a dot.
    if [ "${format}" != Avro ]; then
        table="${format}_dotted"
        create+="
            CREATE TABLE t_dotted (\`a.b\` Nullable(Int32), a Tuple(x Nullable(Int32)),
                t Tuple(\`s.j\` Nullable(Int32), k Nullable(Int32), \`d.e\` Tuple(f Nullable(Int32))))
            ENGINE = IcebergLocal('${table}/', '${format}');
            INSERT INTO t_dotted VALUES (7, (11), (100, 8, (5)));"
        after+="
            SELECT 'dotted', \`a.b\`, a FROM icebergLocal('${table}/');"
        for optimize in 1 0; do
            after+="
                SELECT 'dotted element', ${optimize}, tupleElement(t, 's.j'), t.k FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = ${optimize};
                SELECT 'dotted element', ${optimize}, tupleElement(t, 'd.e'), t.\`d.e\`.f FROM icebergLocal('${table}/') SETTINGS optimize_functions_to_subcolumns = ${optimize};"
        done
    fi

    ${LOCAL} --allow_insert_into_iceberg=1 -q "${create}"
    add_fields "${evolve[@]}"
    ${LOCAL} -q "${before}"
    bind_to_current_schema "${bind[@]}"
    ${LOCAL} -q "${after}"
done

# Malformed defaults.
create=""
evolve=()
select=""
for mode in bad_long bad_struct bad_timestamp bad_timestamptz; do
    create+="
        CREATE TABLE t_${mode} (a Int32, t Tuple(x Nullable(Int32)), l Array(Tuple(x Nullable(Int32)))) ENGINE = IcebergLocal('${mode}/');
        INSERT INTO t_${mode} VALUES (1, (10), [(11)]);"
    evolve+=("${mode}" "${mode}")
    select+="
        SELECT * FROM icebergLocal('${mode}/'); -- { serverError ICEBERG_SPECIFICATION_VIOLATION }"
done
${LOCAL} --allow_insert_into_iceberg=1 -q "${create}"
add_fields "${evolve[@]}"
${LOCAL} -q "SELECT 'malformed defaults'; ${select}"
