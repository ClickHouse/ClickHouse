#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The current snapshot has a `summary` that is a string instead of an object: the bad cast error names both types.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK="$(realpath -m "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}")"
rm -rf "$WORK"
mkdir -p "$WORK/t/metadata"
trap 'rm -rf "$WORK"' EXIT

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`.
cd "$WORK" || exit 1

cat > t/metadata/v1.metadata.json <<'EOF'
{"format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "s3://bucket/test/location",
 "last-sequence-number": 1, "last-updated-ms": 1602638573590, "last-column-id": 1, "current-schema-id": 0,
 "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
 "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
 "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
 "current-snapshot-id": 1, "snapshots": [{"snapshot-id": 1, "timestamp-ms": 1602638573590, "sequence-number": 1,
 "schema-id": 0, "manifest-list": "s3://bucket/test/location/metadata/snap-1.avro", "summary": "append"}]}
EOF

$CLICKHOUSE_LOCAL --query "SELECT * FROM icebergLocal('t')" 2>&1 | grep -o -E 'Can not convert [^ ]+ to [A-Za-z0-9_]+' | sort -u
