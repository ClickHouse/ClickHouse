#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Iceberg tables whose `schema-id` fields are absent: they are optional for the legacy format version 1 `schema`, the schema
# in the Avro header of a format version 1 manifest, and every snapshot; `sort-orders` is optional in format version 1.
# `data_iceberg/impala/iceberg_partitioned` is Apache Impala's `testdata/data/iceberg_test/iceberg_partitioned` at e35eb8f3582,
# written by Spark 2.4 with Iceberg 0.9, which has all of these traits (Apache License 2.0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

T="$CUR_DIR/data_iceberg/impala/iceberg_partitioned"
CORPUS="$CUR_DIR/data_lake_corpus/iceberg/py_v2_schema_evolution"

local_query()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1" < /dev/null
}

# The legacy table as is, including partition pruning and time travel to its snapshot without `schema-id`.
echo '--- legacy'
local_query "DESCRIBE icebergLocal('$T')"
local_query "SELECT * FROM icebergLocal('$T') ORDER BY id"
local_query "SELECT count(), countIf(id > 10), countIf(action != 'click') FROM icebergLocal('$T')"
local_query "SELECT action, count() FROM icebergLocal('$T') WHERE event_time >= '2020-01-01 10:00:00' GROUP BY action ORDER BY action"
local_query "SELECT count() FROM icebergLocal('$T') SETTINGS iceberg_snapshot_id = 8270633197658268308"
local_query "SELECT count() FROM icebergLocal('$T') SETTINGS iceberg_timestamp_ms = 1598853488440"

# The legacy `schema` with a non-empty sort order, as written by Iceberg 0.11.
echo '--- legacy, sorted'
cp -r "$T" "$TMP_DIR/sorted"
python3 -c "
import json, sys
path = sys.argv[1]
metadata = json.load(open(path))
metadata['sort-orders'] = [{'order-id': 1, 'fields': [{'transform': 'identity', 'source-id': 1, 'direction': 'asc', 'null-order': 'nulls-first'}]}]
metadata['default-sort-order-id'] = 1
json.dump(metadata, open(path, 'w'))
" "$TMP_DIR/sorted/metadata/v2.metadata.json"
local_query "SELECT count(), min(id), max(id) FROM icebergLocal('$TMP_DIR/sorted')"

# Format version 2 snapshots whose `schema-id` is absent or null: the data files keep the schema of their manifest, and time travel
# to the first snapshot returns the current schema.
for mode in absent null; do
    echo "--- snapshots, $mode"
    cp -r "$CORPUS" "$TMP_DIR/$mode"
    python3 -c "
import glob, json, sys
path = max(glob.glob(sys.argv[1] + '/metadata/*.metadata.json'))
metadata = json.load(open(path))
for snapshot in metadata['snapshots']:
    if sys.argv[2] == 'absent':
        del snapshot['schema-id']
    else:
        snapshot['schema-id'] = None
json.dump(metadata, open(path, 'w'))
" "$TMP_DIR/$mode" "$mode"
    local_query "SELECT if((SELECT count(), sum(cityHash64(*)) FROM icebergLocal('$TMP_DIR/$mode')) = (SELECT count(), sum(cityHash64(*)) FROM icebergLocal('$CORPUS')), 'ok', 'MISMATCH')"
    local_query "SELECT * FROM icebergLocal('$TMP_DIR/$mode') ORDER BY ALL"
    local_query "DESCRIBE icebergLocal('$TMP_DIR/$mode') SETTINGS iceberg_snapshot_id = 704564496950448094"
    local_query "SELECT * FROM icebergLocal('$TMP_DIR/$mode') ORDER BY ALL SETTINGS iceberg_snapshot_id = 704564496950448094"
done

# `system.iceberg_metadata_log_entries` over snapshots with a null `schema-id`.
echo '--- metadata log entries, null'
local_query "
    CREATE TABLE t_null ENGINE = IcebergLocal('$TMP_DIR/null');
    SELECT count() > 0, countIf(latest_snapshot_id IS NOT NULL AND latest_schema_id IS NOT NULL)
    FROM system.iceberg_metadata_log_entries WHERE table = 't_null';
"
