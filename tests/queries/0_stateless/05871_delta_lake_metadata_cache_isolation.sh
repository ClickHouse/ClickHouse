#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOLDEN="$CUR_DIR/data_delta_lake/kernel_golden"
VERSIONED="deltaLakeLocal('$GOLDEN/basic-with-inserts-deletes-checkpoint/delta')"

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/table" "$WORK_DIR/recreated_log"
cp -r "$GOLDEN/basic-with-inserts-updates/delta" "$WORK_DIR/table/"
TABLE_LOG="$WORK_DIR/table/delta/_delta_log"
RECREATED="deltaLakeLocal('$WORK_DIR/table/delta')"

# The commits of a different table at the same location and version: a new `metaData.id`, and version 1
# keeps the files of version 0 instead of replacing one of them.
sed 's/"id":"testId"/"id":"recreatedId"/' "$TABLE_LOG/00000000000000000000.json" > "$WORK_DIR/recreated_log/00000000000000000000.json"
sed '/"add":{"path":"part-00000-6dfaec75/d; /"remove":{/d' "$TABLE_LOG/00000000000000000001.json" > "$WORK_DIR/recreated_log/00000000000000000001.json"

SCANNED="(SELECT sum(value) FROM system.events WHERE event = 'DeltaLakeScannedFiles')"

$CLICKHOUSE_LOCAL -q "
SET use_delta_lake_metadata_cache = 1;

SELECT 'version 3', count(), sum(id) FROM $VERSIONED SETTINGS delta_lake_snapshot_version = 3;
SELECT 'latest', count(), sum(id) FROM $VERSIONED;
SELECT 'version 3 again', count(), sum(id) FROM $VERSIONED SETTINGS delta_lake_snapshot_version = 3;
SELECT 'version 3 uncached', count(), sum(id) FROM $VERSIONED SETTINGS delta_lake_snapshot_version = 3, use_delta_lake_metadata_cache = 0;
SELECT 'latest uncached', count(), sum(id) FROM $VERSIONED SETTINGS use_delta_lake_metadata_cache = 0;

SELECT 'before recreate', count(), sum(cityHash64(id, str)) FROM $RECREATED;

SET engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('$TABLE_LOG/00000000000000000000.json', RawBLOB)
    SELECT * FROM file('$WORK_DIR/recreated_log/00000000000000000000.json', RawBLOB);
INSERT INTO FUNCTION file('$TABLE_LOG/00000000000000000001.json', RawBLOB)
    SELECT * FROM file('$WORK_DIR/recreated_log/00000000000000000001.json', RawBLOB);

SELECT 'after recreate', count(), sum(cityHash64(id, str)) FROM $RECREATED;
SELECT 'after recreate uncached', count(), sum(cityHash64(id, str)) FROM $RECREATED SETTINGS use_delta_lake_metadata_cache = 0;

SYSTEM CLEAR DELTA LAKE METADATA CACHE;
SELECT 'count cold', count() FROM $RECREATED;
CREATE TABLE scanned (value UInt64) ENGINE = Memory;
INSERT INTO scanned SELECT $SCANNED;
SELECT 'count warm', count() FROM $RECREATED;
SELECT 'warm count scanned files', $SCANNED - (SELECT value FROM scanned);
"

rm -rf "$WORK_DIR"
