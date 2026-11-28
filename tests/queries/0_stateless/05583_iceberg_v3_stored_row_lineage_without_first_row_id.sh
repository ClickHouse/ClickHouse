#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs pyarrow and Iceberg writes.

# A data file without `first_row_id` has NULL `_row_id` and `_last_updated_sequence_number`, also where it stores them.
# https://github.com/ClickHouse/ClickHouse/issues/130021

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A format version 2 table with ids 1..4 in one data file, rewritten to store the given row lineage values.
make_table()
{
    local name=$1 row_ids=$2 sequence_numbers=$3
    local table_path="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_${name}"
    rm -rf "${table_path}"
    ${CLICKHOUSE_CLIENT} --query "
        SET allow_insert_into_iceberg = 1;
        CREATE TABLE t_${name} (id Int64, val Nullable(String)) ENGINE = IcebergLocal('${table_path}');
        INSERT INTO t_${name} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
    "
    ls "${table_path}"/data/*.parquet | wc -l
    python3 - "${table_path}" "${row_ids}" "${sequence_numbers}" <<'PY'
import glob, json, os, re, sys
import pyarrow as pa
import pyarrow.parquet as pq
table_path, row_ids, sequence_numbers = sys.argv[1], json.loads(sys.argv[2]), json.loads(sys.argv[3])
latest = max(glob.glob(os.path.join(table_path, "metadata", "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
with open(latest) as f:
    metadata = json.load(f)
schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
types = {"long": pa.int64(), "string": pa.string()}
fields = [pa.field(field["name"], types[field["type"]], nullable=not field["required"],
                   metadata={b"PARQUET:field_id": str(field["id"]).encode()}) for field in schema["fields"]]
fields.append(pa.field("_row_id", pa.int64(), metadata={b"PARQUET:field_id": b"2147483540"}))
fields.append(pa.field("_last_updated_sequence_number", pa.int64(), metadata={b"PARQUET:field_id": b"2147483539"}))
[data_file] = glob.glob(os.path.join(table_path, "data", "*.parquet"))
rows = pq.read_table(data_file).sort_by("id")
columns = [rows.column(field["name"]) for field in schema["fields"]]
columns += [pa.array(row_ids, pa.int64()), pa.array(sequence_numbers, pa.int64())]
pq.write_table(pa.Table.from_arrays(columns, schema=pa.schema(fields)), data_file)
PY
}

# An external engine upgrades the table to format version 3: a new metadata version with `next-row-id` 0, snapshots untouched.
upgrade_to_v3()
{
    python3 - "$1/metadata" <<'PY'
import glob, json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(glob.glob(os.path.join(metadata_dir, "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
version = int(re.search(r"/v(\d+)\.metadata\.json$", latest).group(1))
with open(latest) as f:
    metadata = json.load(f)
metadata["format-version"] = 3
metadata["next-row-id"] = 0
with open(os.path.join(metadata_dir, f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)
PY
}

first_snapshot_id()
{
    python3 - "$1/metadata" <<'PY'
import glob, json, os, re, sys
latest = max(glob.glob(os.path.join(sys.argv[1], "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
with open(latest) as f:
    metadata = json.load(f)
print(min(metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"])["snapshot-id"])
PY
}

# The v3 commit assigns `first_row_id` to the data written before the upgrade.
insert_v3()
{
    ${CLICKHOUSE_CLIENT} --query "
        SET allow_insert_into_iceberg = 1;
        INSERT INTO TABLE FUNCTION icebergLocal('$1', 'Parquet', 'id Int64, val Nullable(String)') VALUES (5, 'e');
    "
}

STORED="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_stored"
make_table stored '[null, 100, null, 103]' '[null, null, 7, 9]'
${CLICKHOUSE_CLIENT} --query "SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('${STORED}') ORDER BY id"
upgrade_to_v3 "${STORED}"
${CLICKHOUSE_CLIENT} --query "SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('${STORED}') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT id, _last_updated_sequence_number FROM icebergLocal('${STORED}') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT id, _row_id FROM icebergLocal('${STORED}') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${STORED}') WHERE _row_id IS NULL"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${STORED}') WHERE _last_updated_sequence_number IS NULL"
insert_v3 "${STORED}"
${CLICKHOUSE_CLIENT} --query "SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('${STORED}') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "
    SELECT id, val, _row_id, _last_updated_sequence_number FROM icebergLocal('${STORED}') ORDER BY id
    SETTINGS iceberg_snapshot_id = $(first_snapshot_id "${STORED}")"

# Every row stores both columns, so their statistics say that the file has no NULL in them.
COMPLETE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_complete"
make_table complete '[10, 11, 12, 13]' '[5, 5, 5, 5]'
upgrade_to_v3 "${COMPLETE}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${COMPLETE}') WHERE _row_id IS NULL"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${COMPLETE}') WHERE _last_updated_sequence_number IS NULL"
insert_v3 "${COMPLETE}"
# The query condition cache (randomized by the test runner) is on, so the second query could replay the verdict of the first.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM icebergLocal('${COMPLETE}') WHERE id > 0 AND (_row_id IS NULL OR _row_id < 5)
    SETTINGS use_query_condition_cache = 1"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM icebergLocal('${COMPLETE}') WHERE id > 0 AND (_row_id IS NULL OR _row_id < 5)
    SETTINGS use_query_condition_cache = 1, iceberg_snapshot_id = $(first_snapshot_id "${COMPLETE}")"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_stored; DROP TABLE t_complete"
rm -rf "${STORED}" "${COMPLETE}"
