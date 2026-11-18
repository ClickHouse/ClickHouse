#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# An INSERT into an Iceberg table writes the `write-default` of a field for a column it omits, while a data file without the
# field still reads its `initial-default`. Tables written by other engines: `data_iceberg/impala` (Apache Impala test data at
# e35eb8f3582449d736e74ce32f1138f5f5dc085b, Apache-2.0) and `data_iceberg/duckdb` (DuckDB Iceberg extension tests at
# b57f45c2c78965c71f699aa23ae73c292923668e, MIT).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
trap 'rm -rf "${TMP_DIR}"' EXIT

LOCAL="${CLICKHOUSE_LOCAL} --session_timezone UTC --allow_insert_into_iceberg=1"

# A local path must be inside the current directory, so the copies are referred to by relative paths.
cd "${TMP_DIR}" || exit 1
mkdir -p impala duckdb/data/persistent/add_columns_with_defaults
for table in iceberg_v3_default_value test_default_part iceberg_v3_all_types; do
    cp -rp "${CUR_DIR}/data_iceberg/impala/iceberg_v3/${table}" impala/
done
cp -rp impala/iceberg_v3_default_value impala/evolved
cp -rp impala/iceberg_v3_default_value impala/malformed
cp -rp "${CUR_DIR}/data_iceberg/duckdb/data/persistent/add_columns_with_defaults/default.db" duckdb/data/persistent/add_columns_with_defaults/

# For each pair of a table and a mode in $@, adds the next metadata version with a new current schema, and binds every snapshot
# to it: `evolved` drops the `initial-default` of `j` and adds a struct with defaults, `timestamps` adds timestamps with defaults
# before 1973, `malformed` adds an invalid `write-default`.
evolve()
{
    python3 - "$@" <<'PY'
import copy, json, os, re, sys
for table, mode in zip(sys.argv[1::2], sys.argv[2::2]):
    metadata_dir = os.path.join(table, "metadata")
    latest = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
    with open(os.path.join(metadata_dir, f"v{latest}.metadata.json")) as f:
        metadata = json.load(f)
    schema = copy.deepcopy(next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"]))
    schema["schema-id"] = max(s["schema-id"] for s in metadata["schemas"]) + 1
    last_id = metadata["last-column-id"]
    fields = schema["fields"]
    if mode == "evolved":
        del next(f for f in fields if f["name"] == "j")["initial-default"]
        fields.append({"id": last_id + 2, "name": "s", "required": False, "write-default": {}, "type": {"type": "struct", "fields": [
            {"id": last_id + 1, "name": "x", "required": False, "type": "int", "initial-default": 6, "write-default": 5}]}})
        last_id += 2
    elif mode == "timestamps":
        for name, required, type_name, value in (("ts", False, "timestamp", "1970-01-01T00:00:00.012345"),
                ("tr", True, "timestamp", "1970-01-01T00:00:00.012345"), ("tz", False, "timestamptz", "1960-01-01T00:00:00+00:00")):
            last_id += 1
            fields.append({"id": last_id, "name": name, "required": required, "type": type_name,
                "initial-default": value, "write-default": value})
    else:
        last_id += 1
        fields.append({"id": last_id, "name": "d", "required": False, "type": "long", "write-default": "abc"})
    metadata["schemas"].append(schema)
    metadata["current-schema-id"] = schema["schema-id"]
    metadata["last-column-id"] = last_id
    metadata["last-updated-ms"] += 1
    for snapshot in metadata["snapshots"]:
        snapshot["schema-id"] = schema["schema-id"]
    with open(os.path.join(metadata_dir, f"v{latest + 1}.metadata.json"), "w") as f:
        json.dump(metadata, f)
PY
}
evolve impala/evolved evolved impala/malformed malformed

# An omitted field, also of a format, takes its `write-default`; an explicit value or NULL is written as is.
echo '{"i": 103}' | ${LOCAL} -q "INSERT INTO FUNCTION icebergLocal('impala/iceberg_v3_default_value') FORMAT JSONEachRow"
${LOCAL} -q "
    CREATE TABLE t ENGINE = IcebergLocal('impala/iceberg_v3_default_value');
    INSERT INTO t (i) VALUES (100);
    INSERT INTO t VALUES (101, 50), (102, NULL);
    SELECT * FROM t WHERE i >= 100 ORDER BY i;
    DESCRIBE t;
    SELECT 'prewhere', count() > 0 FROM (EXPLAIN actions = 1 SELECT i FROM t WHERE j = -999 SETTINGS optimize_move_to_prewhere = 1)
    WHERE explain ILIKE '%prewhere filter%';
"

# The partition value of an omitted identity partition field is its `write-default`.
${LOCAL} -q "INSERT INTO FUNCTION icebergLocal('impala/test_default_part') (i) VALUES (100)"
manifest=$(ls impala/test_default_part/metadata | grep -v -e '^snap-' -e '-m0\.avro$' | grep '\.avro$')
${LOCAL} -q "
    SELECT 'partition', i, j FROM icebergLocal('impala/test_default_part') WHERE i = 100;
    SELECT 'partition', data_file.partition, data_file.record_count FROM file('impala/test_default_part/metadata/${manifest}', 'Avro');
"

${LOCAL} -q "
    INSERT INTO FUNCTION icebergLocal('impala/iceberg_v3_all_types') (id) VALUES (100);
    SELECT id, col_boolean, col_int, col_bigint, col_float, col_double, col_decimal, col_string, col_date, col_timestamp,
        hex(col_binary), col_timestamp_epoch, col_timestamp_iso
    FROM icebergLocal('impala/iceberg_v3_all_types') WHERE id = 100;
"

cd duckdb || exit 1
${LOCAL} -q "
    INSERT INTO FUNCTION icebergLocal('data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults') (col1) VALUES ('w');
    SELECT col1, col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_time, col_timestamp,
        col_timestamptz, col_string, col_uuid, hex(col_fixed), hex(col_binary)
    FROM icebergLocal('data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults') WHERE col1 = 'w';
"
cd .. || exit 1

# A table definition that holds these DEFAULTs keeps their values when it is parsed back: ALTER validates the definition that
# DESCRIBE loaded, CREATE TABLE ... AS copies it, and a restart reads the copy.
LOCAL_PERSISTENT="${LOCAL} --path chdata"
${LOCAL_PERSISTENT} -q "CREATE TABLE src (k Int32) ENGINE = IcebergLocal('timestamps'); INSERT INTO src VALUES (1)"
evolve timestamps timestamps
${LOCAL_PERSISTENT} -q "
    DESCRIBE src FORMAT Null;
    ALTER TABLE src ADD COLUMN z Nullable(Int32);
    INSERT INTO src (k) VALUES (2);
    SELECT 'alter', k, ts, tr, tz, z FROM src WHERE k = 2;
    CREATE TABLE cp AS src ENGINE = Memory;
"
${LOCAL_PERSISTENT} -q "
    INSERT INTO cp (k) VALUES (3);
    SELECT 'restart', k, ts, tr, tz FROM cp;
"

# A data file without `j` reads NULL when `j` has a `write-default` but no `initial-default`, also in PREWHERE. The members of a
# struct whose `write-default` is `{}` take their own `write-default`.
${LOCAL} -q "
    SELECT 'read', i, j FROM icebergLocal('impala/evolved') WHERE i < 100 ORDER BY i;
    SELECT 'read', count() FROM icebergLocal('impala/evolved') WHERE i < 100 AND j IS NULL SETTINGS optimize_move_to_prewhere = 0;
    SELECT 'read', count() FROM icebergLocal('impala/evolved') WHERE i < 100 AND j IS NULL SETTINGS optimize_move_to_prewhere = 1;
    INSERT INTO FUNCTION icebergLocal('impala/evolved') (i) VALUES (110);
    SELECT 'struct', i, j, s FROM icebergLocal('impala/evolved') WHERE i >= 100;
"

${LOCAL} -q "
    SELECT 'malformed';
    SELECT * FROM icebergLocal('impala/malformed'); -- { serverError ICEBERG_SPECIFICATION_VIOLATION }
"
