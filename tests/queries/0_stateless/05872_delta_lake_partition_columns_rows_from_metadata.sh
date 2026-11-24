#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# A query that reads only partition columns (and virtual columns) of a Delta Lake table takes the number of rows of
# each data file from `numRecords` of the Delta log and does not open the data files.
# https://github.com/ClickHouse/ClickHouse/issues/133705

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

REL="${CLICKHOUSE_TEST_UNIQUE_NAME}_tbl"
DIR="${USER_FILES_PATH:?}/${REL}"
rm -rf "$DIR"
trap 'rm -rf "$DIR"' EXIT

DATA_A="p=1/a.parquet"
DATA_B="p=1/b.parquet"
DATA_C="p=2/c.parquet"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${REL}/${DATA_A}', Parquet, 'x UInt64') SELECT number FROM numbers(3)
    SETTINGS engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${REL}/${DATA_B}', Parquet, 'x UInt64') SELECT number FROM numbers(5)
    SETTINGS engine_file_truncate_on_insert = 1;
    INSERT INTO FUNCTION file('${REL}/${DATA_C}', Parquet, 'x UInt64') SELECT number FROM numbers(7)
    SETTINGS engine_file_truncate_on_insert = 1;"

mkdir -p "$DIR/_delta_log"
python3 - "$DIR" "$DATA_A" "$DATA_B" "$DATA_C" <<'EOF'
import json, os, sys

table_dir = sys.argv[1]
add = lambda path, p, rows: {"add": {
    "path": path, "partitionValues": {"p": p},
    "size": os.path.getsize(os.path.join(table_dir, path)),
    "modificationTime": 1600000000000, "dataChange": True,
    "stats": json.dumps({"numRecords": rows})}}
actions = [
    {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
    {"metaData": {"id": "133705",
                  "format": {"provider": "parquet", "options": {}},
                  "schemaString": json.dumps({"type": "struct", "fields": [
                      {"name": "p", "type": "integer", "nullable": False, "metadata": {}},
                      {"name": "x", "type": "long", "nullable": False, "metadata": {}}]}),
                  "partitionColumns": ["p"],
                  "configuration": {},
                  "createdTime": 1600000000000}},
    add(sys.argv[2], "1", 3),
    add(sys.argv[3], "1", 5),
    add(sys.argv[4], "2", 7),
]
with open(os.path.join(table_dir, "_delta_log", "00000000000000000000.json"), "w") as log:
    for action in actions:
        log.write(json.dumps(action) + "\n")
EOF

run()
{
    local query_id
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$(random_str 10)"
    $CLICKHOUSE_CLIENT --query_id "$query_id" --log_queries 1 -q "$1"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT 'objects read: ' || toString(ProfileEvents['ObjectStorageReadObjects'])
        FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND query_id = '$query_id' AND type = 'QueryFinish'"
}

echo '-- group by a partition column'
run "SELECT p, count() FROM deltaLakeLocal('${REL}') GROUP BY p ORDER BY p"

echo '-- a partition column with a virtual column'
run "SELECT p, _file, count() FROM deltaLakeLocal('${REL}') GROUP BY p, _file ORDER BY _file"

echo '-- filter by a partition column'
run "SELECT p, count() FROM deltaLakeLocal('${REL}') WHERE p = 1 GROUP BY p SETTINGS optimize_move_to_prewhere = 0"

echo '-- optimize_count_from_files = 0'
run "SELECT p, count() FROM deltaLakeLocal('${REL}') GROUP BY p ORDER BY p SETTINGS optimize_count_from_files = 0"

echo '-- a column of the data files'
run "SELECT p, sum(x) FROM deltaLakeLocal('${REL}') GROUP BY p ORDER BY p"
