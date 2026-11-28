#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.
# Tag no-replicated-database: IcebergLocal is not replicated.

# An Iceberg table metadata file without a field that the specification requires for its format version,
# or with a field of the wrong type, is rejected with ICEBERG_SPECIFICATION_VIOLATION naming the field.
# A file that the JSON parser cannot read is rejected with ICEBERG_SPECIFICATION_VIOLATION naming the file.
# Every case is the valid file of `table()` with one change. Only the file a table uses is checked: another table's
# metadata file in the same directory, read to select by `table-uuid`, may be invalid.
# A metadata file served from the metadata cache is validated once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"
trap 'rm -rf "$DIR"' EXIT

python3 - "$DIR" <<'PY'
import json
import os
import sys

root = sys.argv[1]
TIMESTAMP = 1750772174828
NOT_LONG = 9876543210123456789


def table(version=2):
    schema = {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}]}
    t = {
        "format-version": version,
        "table-uuid": "3914c1f5-bf97-4265-8c4c-78cd91736b19",
        "location": "",
        "last-sequence-number": 0,
        "last-updated-ms": TIMESTAMP,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [schema],
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
    if version == 1:
        # Format version 1 without the fields that only later versions require.
        for key in ("table-uuid", "last-sequence-number", "last-partition-id"):
            del t[key]
        t["schema"] = schema
        t["partition-spec"] = []
    return t


def snapshot(snapshot_id=1):
    return {
        "snapshot-id": snapshot_id,
        "sequence-number": 1,
        "timestamp-ms": TIMESTAMP,
        "manifest-list": "snap-1.avro",
        "schema-id": 0,
        "summary": {"operation": "append"},
    }


def change(t, **fields):
    for key, value in fields.items():
        key = key.replace("_", "-")
        if value is DELETE:
            del t[key]
        else:
            t[key] = value
    return t


DELETE = object()

cases = {}

# Valid files.
cases["valid_v2"] = table()
cases["valid_v1"] = table(1)
cases["valid_v3_without_next_row_id"] = table(3)
null_optionals = dict(snapshot(), **{"parent-snapshot-id": None, "schema-id": None, "first-row-id": None})
cases["valid_v2_null_optionals"] = change(
    table(),
    current_snapshot_id=None,
    snapshots=[null_optionals],
    snapshot_log=[{"snapshot-id": 1, "timestamp-ms": TIMESTAMP}],
    metadata_log=[{"metadata-file": "v0.metadata.json", "timestamp-ms": TIMESTAMP}])

# An unsupported version, where `location` is optional.
cases["format_version_4"] = change(table(), format_version=4, location=DELETE)

# Invalid files.
cases["missing_format_version"] = change(table(), format_version=DELETE)
cases["format_version_0"] = change(table(), format_version=0)
cases["format_version_string"] = change(table(), format_version="2")
cases["missing_location"] = change(table(), location=DELETE)
cases["location_not_string"] = change(table(), location=1)
cases["missing_last_updated_ms"] = change(table(), last_updated_ms=DELETE)
cases["last_column_id_float"] = change(table(), last_column_id=1.5)
cases["last_partition_id_out_of_int32"] = change(table(), last_partition_id=2**31)
cases["v2_missing_table_uuid"] = change(table(), table_uuid=DELETE)
cases["v2_missing_last_sequence_number"] = change(table(), last_sequence_number=DELETE)
cases["v2_missing_partition_specs"] = change(table(), partition_specs=DELETE, default_spec_id=DELETE, partition_spec=[])
cases["v2_missing_last_partition_id"] = change(table(), last_partition_id=DELETE)
cases["v2_missing_sort_orders"] = change(table(), sort_orders=DELETE, default_sort_order_id=DELETE)
cases["missing_default_spec_id"] = change(table(), default_spec_id=DELETE)
cases["missing_default_sort_order_id"] = change(table(), default_sort_order_id=DELETE)
cases["v1_missing_partition_spec"] = change(table(1), partition_specs=DELETE, default_spec_id=DELETE, partition_spec=DELETE)
cases["missing_spec_id"] = change(table(), partition_specs=[{"fields": []}])
cases["sort_order_missing_order_id"] = change(table(), sort_orders=[{"fields": []}])
cases["current_snapshot_id_out_of_range"] = change(
    table(),
    current_snapshot_id=NOT_LONG,
    snapshots=[snapshot(NOT_LONG)],
    refs={"main": {"snapshot-id": NOT_LONG, "type": "branch"}})
cases["snapshot_id_out_of_range"] = change(table(), snapshots=[snapshot(NOT_LONG)])
# Out of the range of both `long` and unsigned `long`, so the JSON parser rejects it.
cases["current_snapshot_id_below_long"] = change(table(), current_snapshot_id=-2**63 - 1)
cases["current_snapshot_id_above_unsigned_long"] = change(table(), current_snapshot_id=2**64)
cases["snapshot_missing_timestamp"] = change(table(), snapshots=[change(snapshot(), timestamp_ms=DELETE)])
cases["snapshot_not_object"] = change(table(), snapshots=[1])
cases["snapshots_not_array"] = change(table(), snapshots={})
cases["snapshot_log_missing_snapshot_id"] = change(table(), snapshot_log=[{"timestamp-ms": TIMESTAMP}])
cases["metadata_log_missing_file"] = change(table(), metadata_log=[{"timestamp-ms": TIMESTAMP}])
cases["ref_missing_type"] = change(table(), refs={"main": {"snapshot-id": 1}})
cases["refs_not_object"] = change(table(), refs=[])

# Partition fields and sort fields.
identity_field = {"source-id": 1, "field-id": 1000, "name": "id", "transform": "identity"}
sort_field = {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"}
cases["valid_v3_source_ids"] = change(
    table(3),
    partition_specs=[
        {"spec-id": 0, "fields": []},
        {"spec-id": 1, "fields": [{"source-ids": [1], "field-id": 1000, "name": "id_bucket", "transform": "bucket[4]"}]}],
    last_partition_id=1000,
    sort_orders=[
        {"order-id": 0, "fields": []},
        {"order-id": 1, "fields": [{"transform": "bucket[4]", "source-ids": [1], "direction": "asc", "null-order": "nulls-first"}]}])
cases["partition_field_missing_transform"] = change(
    table(), partition_specs=[{"spec-id": 0, "fields": [change(dict(identity_field), transform=DELETE)]}], last_partition_id=1000)
cases["partition_field_missing_name"] = change(
    table(), partition_specs=[{"spec-id": 0, "fields": [change(dict(identity_field), name=DELETE)]}], last_partition_id=1000)
cases["partition_field_missing_source_id"] = change(
    table(), partition_specs=[{"spec-id": 0, "fields": [change(dict(identity_field), source_id=DELETE)]}], last_partition_id=1000)
cases["partition_field_not_object"] = change(table(), partition_specs=[{"spec-id": 0, "fields": [1]}])
cases["sort_field_missing_null_order"] = change(
    table(), sort_orders=[{"order-id": 1, "fields": [change(dict(sort_field), null_order=DELETE)]}], default_sort_order_id=1)
cases["sort_field_missing_source_id"] = change(
    table(), sort_orders=[{"order-id": 1, "fields": [change(dict(sort_field), source_id=DELETE)]}], default_sort_order_id=1)
cases["v1_partition_spec_field_missing_name"] = change(
    table(1), partition_specs=DELETE, default_spec_id=DELETE, partition_spec=[change(dict(identity_field), name=DELETE)])
cases["v2_partition_field_source_ids_only"] = change(
    table(), partition_specs=[{"spec-id": 0, "fields": [change(dict(identity_field), source_id=DELETE, source_ids=[1])]}], last_partition_id=1000)
cases["v1_sort_field_source_ids_only"] = change(
    table(1), sort_orders=[{"order-id": 1, "fields": [change(dict(sort_field), source_id=DELETE, source_ids=[1])]}], default_sort_order_id=1)

# Selection by `last-updated-ms` reads it from every metadata file.
cases["missing_last_updated_ms_by_field"] = change(table(), last_updated_ms=DELETE)
cases["negative_last_updated_ms_by_field"] = change(table(), last_updated_ms=-1)

for name, metadata in cases.items():
    os.makedirs(os.path.join(root, name, "metadata"))
    if "location" in metadata and metadata["location"] == "":
        metadata["location"] = os.path.join(root, name)
    with open(os.path.join(root, name, "metadata", "v1.metadata.json"), "w") as f:
        json.dump(metadata, f)

# Two tables in one directory, selected by `table-uuid`: `v1` of a valid table and `v2` of a table without `location`.
os.makedirs(os.path.join(root, "shared", "metadata"))
valid = change(table(), location=os.path.join(root, "shared"))
invalid = change(table(), table_uuid="a90eed4c-f74b-4e5b-b630-096fb9d09021", location=DELETE)
for version, metadata in ((1, valid), (2, invalid)):
    with open(os.path.join(root, "shared", "metadata", f"v{version}.metadata.json"), "w") as f:
        json.dump(metadata, f)
PY

# Prints the table, the error code and the field the error names for each exception line of stdin.
function rejections()
{
    python3 -c '
import re
import sys

for line in sys.stdin:
    if "DB::Exception" not in line:
        continue
    table = re.search(r"/([^/]+)/metadata/[^/]+\.metadata\.json", line)
    code = re.findall(r"\(([A-Z_]+)\)", line)
    field = re.search(r"field (\S+)", line)
    print(table.group(1) if table else "?", code[-1] if code else "?", field.group(1).rstrip(":.") if field else "?")
'
}

echo "--- valid"
for name in valid_v2 valid_v1 valid_v3_without_next_row_id valid_v2_null_optionals valid_v3_source_ids
do
    echo "$name $($CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$DIR/$name')" 2>&1)"
done
echo "negative_last_updated_ms_by_field $($CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$DIR/negative_last_updated_ms_by_field', SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 1)" 2>&1)"
echo "shared $($CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$DIR/shared', SETTINGS iceberg_metadata_table_uuid = '3914c1f5-bf97-4265-8c4c-78cd91736b19')" 2>&1)"

echo "--- unsupported"
echo "format_version_4 $($CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$DIR/format_version_4')" 2>&1 | grep -o -m1 '(NOT_IMPLEMENTED)')"

echo "--- invalid"
QUERIES=""
for name in missing_format_version format_version_0 format_version_string missing_location location_not_string \
    missing_last_updated_ms last_column_id_float last_partition_id_out_of_int32 v2_missing_table_uuid v2_missing_last_sequence_number \
    v2_missing_partition_specs v2_missing_last_partition_id v2_missing_sort_orders missing_default_spec_id \
    missing_default_sort_order_id v1_missing_partition_spec missing_spec_id sort_order_missing_order_id \
    partition_field_missing_transform partition_field_missing_name partition_field_missing_source_id partition_field_not_object \
    sort_field_missing_null_order sort_field_missing_source_id v1_partition_spec_field_missing_name \
    v2_partition_field_source_ids_only v1_sort_field_source_ids_only \
    current_snapshot_id_out_of_range snapshot_id_out_of_range current_snapshot_id_below_long current_snapshot_id_above_unsigned_long \
    snapshot_missing_timestamp snapshot_not_object \
    snapshots_not_array snapshot_log_missing_snapshot_id metadata_log_missing_file ref_missing_type refs_not_object
do
    QUERIES+="SELECT count() FROM icebergLocal('$DIR/$name');"
done
QUERIES+="SELECT count() FROM icebergLocal('$DIR/missing_last_updated_ms_by_field', SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 1);"
QUERIES+="SELECT count() FROM icebergLocal('$DIR/shared', SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');"

$CLICKHOUSE_LOCAL --ignore-error --multiquery --query "$QUERIES" 2>&1 >/dev/null | rejections

echo "--- a table whose next metadata file is invalid, with and without the metadata cache"
rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
for cache in 1 0
do
    name="cache_${cache}"
    TABLE="t_${name}_${CLICKHOUSE_TEST_UNIQUE_NAME}"
    TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/${name}/"
    CLIENT="$CLICKHOUSE_CLIENT --use_iceberg_metadata_files_cache=$cache"
    $CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}_create" --query "CREATE TABLE $TABLE (id Int64) ENGINE = IcebergLocal('$TABLE_PATH')"
    echo "$name $($CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}_read" --query "SELECT count() FROM $TABLE")"
    python3 - "${TABLE_PATH}metadata" <<'PY'
import json
import os
import re
import sys

metadata_dir = sys.argv[1]
version = max(int(m.group(1)) for m in (re.fullmatch(r"v(\d+)\.metadata\.json", f) for f in os.listdir(metadata_dir)) if m)
with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
    metadata = json.load(f)
del metadata["location"]
with open(os.path.join(metadata_dir, f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY
    $CLIENT --send_logs_level=fatal --iceberg_metadata_staleness_ms=0 --query "SELECT count() FROM $TABLE" 2>&1 | rejections
    $CLICKHOUSE_CLIENT --query "DROP TABLE $TABLE"
done
rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

echo "--- a metadata file is validated after it is parsed, and not again when it is served from the metadata cache"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
for cache in 1 0
do
    $CLICKHOUSE_CLIENT --query "
        SELECT 'cache_${cache}', validations > 0, if(${cache}, validations <= parses AND hits > 0, validations = parses AND parses > 1)
        FROM
        (
            SELECT
                sum(ProfileEvents['IcebergMetadataJSONValidations']) AS validations,
                sum(ProfileEvents['IcebergMetadataJSONParses']) AS parses,
                sum(ProfileEvents['IcebergMetadataFilesCacheHits']) AS hits
            FROM system.query_log
            WHERE current_database = currentDatabase() AND type = 'QueryFinish'
                AND query_id IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_${cache}_create', '${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_${cache}_read')
        )"
done
