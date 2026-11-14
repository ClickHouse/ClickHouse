#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# The change data feed (`delta_lake_snapshot_start_version`) of Delta Lake tables, read like the data files of a snapshot,
# with the transform of every file:
# - a partition column of type `Decimal` with a precision above 18 (a 128-bit literal in the transform);
# - a subcolumn of a struct with column mapping, which is requested by its physical name;
# - a filter on a column of the change data feed, when some files (CDC files) read the `_change_type` column and others not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"
mkdir -p "$DIR/mapped/_delta_log" "$DIR/plain/_delta_log"

# Data files use the physical names and do not contain the partition column.
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$DIR/mapped/p1.parquet', Parquet, '\`c-id\` Int32, \`c-t\` Tuple(\`c-t-x\` Nullable(Int32), \`c-t-y\` Nullable(String))')
VALUES (1, (10, 'a')), (2, (20, 'b'));
INSERT INTO FUNCTION file('$DIR/mapped/p2.parquet', Parquet, '\`c-id\` Int32, \`c-t\` Tuple(\`c-t-x\` Nullable(Int32), \`c-t-y\` Nullable(String))')
VALUES (3, (NULL, 'c'));
INSERT INTO FUNCTION file('$DIR/plain/p1.parquet', Parquet, 'id Int32, t Tuple(x Nullable(Int32), y Nullable(String))')
VALUES (1, (10, 'a')), (2, (20, 'b'));
INSERT INTO FUNCTION file('$DIR/plain/p2.parquet', Parquet, 'id Int32, t Tuple(x Nullable(Int32), y Nullable(String))')
VALUES (3, (NULL, 'c'));
"

python3 - "$DIR" <<'EOF'
import json, os, sys

directory = sys.argv[1]

def write_log(table, mapped):
    max_id = 0
    def field(name, type_, physical):
        nonlocal max_id
        metadata = {}
        if mapped:
            max_id += 1
            metadata = {"delta.columnMapping.id": max_id, "delta.columnMapping.physicalName": physical}
        return {"name": name, "type": type_, "nullable": True, "metadata": metadata}

    schema = {"type": "struct", "fields": [
        field("id", "integer", "c-id"),
        field("t", {"type": "struct", "fields": [field("x", "integer", "c-t-x"), field("y", "string", "c-t-y")]}, "c-t"),
        field("p", "decimal(25,2)", "c-p"),
    ]}
    configuration = {"delta.enableChangeDataFeed": "true"}
    if mapped:
        protocol = {"minReaderVersion": 2, "minWriterVersion": 5}
        configuration["delta.columnMapping.mode"] = "name"
        configuration["delta.columnMapping.maxColumnId"] = str(max_id)
    else:
        protocol = {"minReaderVersion": 1, "minWriterVersion": 4}
    partition_column = "c-p" if mapped else "p"

    table_dir = os.path.join(directory, table)
    def add(path, value, rows):
        return {"add": {"path": path, "partitionValues": {partition_column: value},
                        "size": os.path.getsize(os.path.join(table_dir, path)),
                        "modificationTime": 1700000000000, "dataChange": True,
                        "stats": json.dumps({"numRecords": rows})}}

    versions = [
        [
            {"protocol": protocol},
            {"metaData": {"id": table, "format": {"provider": "parquet", "options": {}},
                          "schemaString": json.dumps(schema), "partitionColumns": ["p"],
                          "configuration": configuration, "createdTime": 1700000000000}},
            add("p1.parquet", "1.23", 2),
        ],
        [add("p2.parquet", "-12345678901234567890.45", 1)],
    ]
    for version, actions in enumerate(versions):
        actions = [{"commitInfo": {"timestamp": 1700000000000 + version, "operation": "WRITE"}}] + actions
        with open(os.path.join(table_dir, "_delta_log", "%020d.json" % version), "w") as log:
            for action in actions:
                log.write(json.dumps(action) + "\n")

write_log("mapped", True)
write_log("plain", False)
EOF

for T in mapped plain; do
$CLICKHOUSE_LOCAL -q "
SELECT '--- $T';
SELECT 'partition values';
SELECT id, p, toTypeName(p) FROM deltaLakeLocal('$DIR/$T') ORDER BY id;
SELECT id, p, _change_type, _commit_version FROM deltaLakeLocal('$DIR/$T') ORDER BY id SETTINGS delta_lake_snapshot_start_version = 0;
SELECT id FROM deltaLakeLocal('$DIR/$T') WHERE p < 0 ORDER BY id SETTINGS delta_lake_snapshot_start_version = 0;
SELECT 'subcolumns';
SELECT id, t.x, t.y FROM deltaLakeLocal('$DIR/$T') ORDER BY id;
SELECT id, t.x, t.y, p FROM deltaLakeLocal('$DIR/$T') ORDER BY id SETTINGS delta_lake_snapshot_start_version = 0;
SELECT id, t, t.x FROM deltaLakeLocal('$DIR/$T') ORDER BY id;
SELECT id, t, t.x, t.x.null FROM deltaLakeLocal('$DIR/$T') WHERE t.x > 10 ORDER BY id SETTINGS delta_lake_snapshot_start_version = 0;
SELECT id, t FROM deltaLakeLocal('$DIR/$T') WHERE t.x > 10 ORDER BY id SETTINGS delta_lake_snapshot_start_version = 0;
SELECT id, t.x.null, p FROM deltaLakeLocal('$DIR/$T') ORDER BY id;
SELECT id, t.x.null, t.x, t.y FROM deltaLakeLocal('$DIR/$T') WHERE t.x > 10 OR t.x IS NULL ORDER BY id SETTINGS delta_lake_snapshot_start_version = 0;
"
done

rm -rf "$DIR"

# A filter on a column of the change data feed, over CDC files (with a physical `_change_type` column) and data files.
TABLE="$CUR_DIR/data_lake_corpus/delta/change_data_feed_deletion_vectors"
$CLICKHOUSE_LOCAL -q "
SELECT '--- filter on the columns of the change data feed';
SELECT _commit_version, _change_type, id, s FROM deltaLakeLocal('$TABLE')
    WHERE _commit_version >= 2 ORDER BY _commit_version, id, _change_type SETTINGS delta_lake_snapshot_start_version = 2;
SELECT _commit_version, _change_type, count() FROM deltaLakeLocal('$TABLE')
    WHERE _change_type != 'insert' AND _commit_version > 0 GROUP BY ALL ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 0;
SELECT count() FROM deltaLakeLocal('$TABLE')
    WHERE _commit_version >= 2 SETTINGS delta_lake_snapshot_start_version = 0, max_threads = 1;
"
