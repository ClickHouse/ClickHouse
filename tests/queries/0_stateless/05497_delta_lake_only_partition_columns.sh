#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies, and MinIO is not in fast test.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/129211
# A Delta table whose columns are all partition columns (what Spark leaves after DROP COLUMN
# of the last non-partition column, with column mapping) must be readable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"

# The data files were written before the DROP COLUMN, so they hold only the dropped
# column 'c0' under its physical name.
DATA_A="col_c2=a/col_id=1/part-00000-a.parquet"
DATA_B="col_c2=b/col_id=2/part-00000-b.parquet"
mkdir -p "$DIR/t/_delta_log" "$DIR/t/col_c2=a/col_id=1" "$DIR/t/col_c2=b/col_id=2"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/t/$DATA_A', Parquet, 'col_c0 String')
    SELECT toString(number) FROM numbers(2)
    SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$DIR/t/$DATA_B', Parquet, 'col_c0 String')
    SELECT toString(number) FROM numbers(1)
    SETTINGS engine_file_truncate_on_insert = 1"

python3 - "$DIR/t" "$DATA_A" "$DATA_B" <<'EOF'
import json, os, sys

table_dir, data_a, data_b = sys.argv[1], sys.argv[2], sys.argv[3]
mapped = lambda name, type_, id_, physical: {
    "name": name, "type": type_, "nullable": True,
    "metadata": {"delta.columnMapping.id": id_, "delta.columnMapping.physicalName": physical}}
add = lambda path, c2, id_, rows: {"add": {
    "path": path, "partitionValues": {"col_c2": c2, "col_id": id_},
    "size": os.path.getsize(os.path.join(table_dir, path)),
    "modificationTime": 1600000000000, "dataChange": True,
    "stats": json.dumps({"numRecords": rows})}}
actions = [
    {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
    {"metaData": {"id": "129211",
                  "format": {"provider": "parquet", "options": {}},
                  "schemaString": json.dumps({"type": "struct", "fields": [
                      mapped("id", "long", 1, "col_id"),
                      mapped("c2", "string", 4, "col_c2")]}),
                  "partitionColumns": ["c2", "id"],
                  "configuration": {"delta.columnMapping.mode": "id",
                                    "delta.columnMapping.maxColumnId": "4"},
                  "createdTime": 1600000000000}},
    add(data_a, "a", "1", 2),
    add(data_b, "b", "2", 1),
]
with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in actions:
        log.write(json.dumps(action) + "\n")
EOF

echo '-- only partition columns'
$CLICKHOUSE_LOCAL -q "SELECT id, c2 FROM deltaLakeLocal('$DIR/t') ORDER BY id"

echo '-- count() without the trivial count optimization'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DIR/t') SETTINGS optimize_trivial_count_query = 0"

echo '-- only virtual columns'
$CLICKHOUSE_LOCAL -q "SELECT _file, count() FROM deltaLakeLocal('$DIR/t') GROUP BY _file ORDER BY _file"

# The same table on S3.
S3_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}/t"
for file in "_delta_log/00000000000000000000.json" "$DATA_A" "$DATA_B"; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION s3(s3_conn, filename = '$S3_DIR/$file', format = RawBLOB)
        SETTINGS s3_truncate_on_insert = 1, use_hive_partitioning = 0 FORMAT RawBLOB" < "$DIR/t/$file"
done

echo '-- s3: only partition columns'
$CLICKHOUSE_CLIENT -q "SELECT id, c2 FROM deltaLake(s3_conn, filename = '$S3_DIR/') ORDER BY id"

echo '-- s3: count() without the trivial count optimization'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM deltaLake(s3_conn, filename = '$S3_DIR/') SETTINGS optimize_trivial_count_query = 0"

echo '-- s3: only virtual columns'
$CLICKHOUSE_CLIENT -q "SELECT _file, count() FROM deltaLake(s3_conn, filename = '$S3_DIR/') GROUP BY _file ORDER BY _file"

rm -rf "$DIR"
