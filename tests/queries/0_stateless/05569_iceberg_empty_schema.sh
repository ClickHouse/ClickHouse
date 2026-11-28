#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# A table needs at least one column, so an Iceberg schema without fields cannot be the schema of a read:
# the error names the schema-id instead of claiming that the schema is unknown. An old schema without fields
# does not prevent reading the table with a later schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"/{v2_empty,v1_empty,old_empty}/metadata
trap 'rm -rf "$DIR"' EXIT

err() { $CLICKHOUSE_LOCAL --query "$1" 2>&1 | grep -o -E 'Code: [0-9]+|schema-id [0-9]+ has no fields' | sort -u; }

cat > "$DIR/v2_empty/metadata/v1.metadata.json" <<'EOF'
{"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "file:///unused",
 "last-sequence-number": 0, "last-updated-ms": 1, "last-column-id": 0,
 "current-schema-id": 0, "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
 "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
 "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
 "properties": {}, "snapshots": [], "snapshot-log": [], "metadata-log": []}
EOF

cat > "$DIR/v1_empty/metadata/v1.metadata.json" <<'EOF'
{"format-version": 1, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c2", "location": "file:///unused",
 "last-updated-ms": 1, "last-column-id": 0,
 "schema": {"type": "struct", "schema-id": 0, "fields": []},
 "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
 "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
 "properties": {}, "snapshots": [], "snapshot-log": [], "metadata-log": []}
EOF

cat > "$DIR/old_empty/metadata/v1.metadata.json" <<'EOF'
{"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c3", "location": "file:///unused",
 "last-sequence-number": 0, "last-updated-ms": 1, "last-column-id": 1,
 "current-schema-id": 1, "schemas": [{"type": "struct", "schema-id": 0, "fields": []},
     {"type": "struct", "schema-id": 1, "fields": [{"id": 1, "name": "a", "required": false, "type": "int"}]}],
 "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
 "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
 "properties": {}, "snapshots": [], "snapshot-log": [], "metadata-log": []}
EOF

# The first snapshot of a corpus table is bound to a new schema without fields.
cp -r "$CUR_DIR/data_lake_corpus/iceberg/py_v2_schema_evolution" "$DIR/time_travel"
python3 - "$DIR/time_travel/metadata" <<'EOF'
import glob, json, os, sys
[path] = glob.glob(os.path.join(sys.argv[1], "00005-*.metadata.json"))
with open(path) as f:
    metadata = json.load(f)
metadata["schemas"].append({"type": "struct", "schema-id": 3, "fields": []})
for snapshot in metadata["snapshots"]:
    if snapshot["snapshot-id"] == 704564496950448094:
        snapshot["schema-id"] = 3
with open(path, "w") as f:
    json.dump(metadata, f)
EOF

echo 'current schema has no fields'
err "DESCRIBE icebergLocal('$DIR/v2_empty')"

echo 'v1 legacy schema has no fields'
err "DESCRIBE icebergLocal('$DIR/v1_empty')"

echo 'old empty schema, current schema has a column'
$CLICKHOUSE_LOCAL --query "DESCRIBE icebergLocal('$DIR/old_empty') SETTINGS describe_compact_output = 1"
$CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$DIR/old_empty')"

echo 'snapshot schema has no fields'
err "DESCRIBE icebergLocal('$DIR/time_travel') SETTINGS iceberg_snapshot_id = 704564496950448094"
$CLICKHOUSE_LOCAL --query "SELECT count() FROM icebergLocal('$DIR/time_travel')"
