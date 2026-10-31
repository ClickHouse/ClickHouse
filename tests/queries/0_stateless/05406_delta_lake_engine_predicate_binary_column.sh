#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# A Delta `binary` column is read as `String`. A filter on it is pushed down to delta-kernel, which must not
# compare the column with a string literal (`Invalid comparison operation: BinaryView <= Utf8View`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_binary"
trap 'rm -rf "${ROOT}"' EXIT
rm -rf "${ROOT}"
for t in plain stats part nested cdf dotted; do
    mkdir -p "${ROOT}/${t}/_delta_log"
done

# Strings are written as unannotated BYTE_ARRAY, as Spark writes `binary`. `dotted` uses column mapping physical names.
${CLICKHOUSE_LOCAL} --output_format_parquet_string_as_string=0 --query "
INSERT INTO FUNCTION file('${ROOT}/plain/part-0.parquet', Parquet, 'b String, s String') SELECT 'x', 'x';
INSERT INTO FUNCTION file('${ROOT}/stats/f0.parquet', Parquet, 'b String, s String') SELECT 'x', 'x';
INSERT INTO FUNCTION file('${ROOT}/stats/f1.parquet', Parquet, 'b String, s String') SELECT 'x', 'y';
INSERT INTO FUNCTION file('${ROOT}/part/x.parquet', Parquet, 's String') SELECT 'x';
INSERT INTO FUNCTION file('${ROOT}/part/y.parquet', Parquet, 's String') SELECT 'y';
INSERT INTO FUNCTION file('${ROOT}/nested/part-0.parquet', Parquet, 't Tuple(b String)') SELECT tuple('x');
INSERT INTO FUNCTION file('${ROOT}/cdf/part-0.parquet', Parquet, 'b String') SELECT 'x';
INSERT INTO FUNCTION file('${ROOT}/dotted/part-0.parquet', Parquet,
    '\`c-ab\` String, \`c-a\` Tuple(\`c-a-b\` String), \`c-xy\` String, \`c-x\` Tuple(\`c-x-y\` String)')
    SELECT 'x', tuple('x'), 'x', tuple('z');
"

python3 - "${ROOT}" <<'EOF'
import json, os, sys

root = sys.argv[1]
max_id = 0

def field(name, type_, physical=None):
    global max_id
    metadata = {}
    if physical is not None:
        max_id += 1
        metadata = {"delta.columnMapping.id": max_id, "delta.columnMapping.physicalName": physical}
    return {"name": name, "type": type_, "nullable": True, "metadata": metadata}

def struct(*fields):
    return {"type": "struct", "fields": list(fields)}

def write_log(table, fields, files, partition_columns=(), protocol=(1, 2), configuration=None):
    actions = [
        {"protocol": {"minReaderVersion": protocol[0], "minWriterVersion": protocol[1]}},
        {"metaData": {"id": table, "format": {"provider": "parquet", "options": {}},
                      "schemaString": json.dumps(struct(*fields)), "partitionColumns": list(partition_columns),
                      "configuration": configuration or {}, "createdTime": 1700000000000}},
    ]
    for path, partition_values, stats in files:
        add = {"path": path, "partitionValues": partition_values,
               "size": os.path.getsize(os.path.join(root, table, path)),
               "modificationTime": 1700000000000, "dataChange": True}
        if stats is not None:
            add["stats"] = json.dumps(stats)
        actions.append({"add": add})
    with open(os.path.join(root, table, "_delta_log", "00000000000000000000.json"), "w") as log:
        for action in actions:
            log.write(json.dumps(action) + "\n")

# Delta writers record no min/max for `binary` columns.
def string_stats(column, value):
    return {"numRecords": 1, "minValues": {column: value}, "maxValues": {column: value}, "nullCount": {column: 0}}

write_log("plain", [field("b", "binary"), field("s", "string")], [("part-0.parquet", {}, None)])
write_log("stats", [field("b", "binary"), field("s", "string")],
          [("f0.parquet", {}, string_stats("s", "x")), ("f1.parquet", {}, string_stats("s", "y"))])
write_log("part", [field("p", "binary"), field("s", "string")],
          [("x.parquet", {"p": "x"}, None), ("y.parquet", {"p": "y"}, None)], ["p"])
write_log("nested", [field("t", struct(field("b", "binary")))], [("part-0.parquet", {}, None)])
write_log("cdf", [field("b", "binary")], [("part-0.parquet", {}, None)],
          protocol=(1, 4), configuration={"delta.enableChangeDataFeed": "true"})
# delta-kernel splits a column name at every dot: `a.b` and `x.y` also name the struct fields.
fields = [
    field("a.b", "string", "c-ab"),
    field("a", struct(field("b", "binary", "c-a-b")), "c-a"),
    field("x.y", "binary", "c-xy"),
    field("x", struct(field("y", "string", "c-x-y")), "c-x"),
]
write_log("dotted", fields,
          [("part-0.parquet", {}, {"numRecords": 1, "minValues": {"c-x": {"c-x-y": "z"}},
                                   "maxValues": {"c-x": {"c-x-y": "z"}}, "nullCount": {"c-x": {"c-x-y": 0}}})],
          protocol=(2, 5), configuration={"delta.columnMapping.mode": "name", "delta.columnMapping.maxColumnId": str(max_id)})
EOF

${CLICKHOUSE_LOCAL} --query "
SELECT 'b', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE b = 'x';
SELECT 's', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE s = 'x';
SELECT 'b, no engine predicate', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE b = 'x' SETTINGS delta_lake_enable_engine_predicate = 0;
SELECT 'b > a', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE b > 'a';
SELECT 'y = b', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE 'y' = b;
SELECT 'b, throw on engine predicate error', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE b = 'x' SETTINGS delta_lake_throw_on_engine_predicate_error = 1;
SELECT 'b, non-UTF-8 constant', count() FROM deltaLakeLocal('${ROOT}/plain') WHERE b != unhex('FF') SETTINGS delta_lake_throw_on_engine_predicate_error = 1;
SELECT 'struct field', count() FROM deltaLakeLocal('${ROOT}/nested') WHERE t.b = 'x';
SELECT 'change data feed', count() FROM deltaLakeLocal('${ROOT}/cdf') WHERE b = 'x' SETTINGS delta_lake_snapshot_start_version = 0;
SELECT 'a.b', count() FROM deltaLakeLocal('${ROOT}/dotted') WHERE \`a.b\` = 'x';
SELECT 'x.y', count() FROM deltaLakeLocal('${ROOT}/dotted') WHERE \`x.y\` = 'x';
"

# Rows and files returned by delta-kernel (DeltaLakeScannedFiles is counted before ClickHouse's own partition pruning).
LABELS=('stats: b and s' 'part: p' 'part: p, FixedString constant' 'part: p declared as FixedString(2)')
QUERIES=(
    "deltaLakeLocal('${ROOT}/stats') WHERE b = 'x' AND s = 'y'"
    "deltaLakeLocal('${ROOT}/part') WHERE p = 'x'"
    "deltaLakeLocal('${ROOT}/part') WHERE p = toFixedString('x', 1)"
    "part_fixed WHERE p = 'x'"
)
BATCH="CREATE TABLE part_fixed (p FixedString(2), s Nullable(String)) ENGINE = DeltaLakeLocal('${ROOT}/part', Parquet);
    SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"
for query in "${QUERIES[@]}"; do
    BATCH+="SELECT count() FROM ${query}; SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"
done
OUT=$(${CLICKHOUSE_LOCAL} --query "${BATCH}")
mapfile -t LINES <<< "${OUT}"
for i in "${!LABELS[@]}"; do
    printf '%s\t%s\t%s\n' "${LABELS[$i]}" "${LINES[2 * i + 1]}" "$(( LINES[2 * i + 2] - LINES[2 * i] ))"
done
