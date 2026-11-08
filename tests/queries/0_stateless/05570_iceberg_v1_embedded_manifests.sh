#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# A format version 1 snapshot may list its manifest files in `manifests` instead of pointing to a manifest list file.
# `data_iceberg/duckdb/data/persistent/iceberg_v1_deprecated/default/legacy_v1` is from duckdb/duckdb-iceberg at b57f45c2c789 (MIT License).
# Its `v2.metadata.json` also has only the deprecated `schema` and `partition-spec`; the copies add `schemas`, `partition-specs` and
# `sort-orders`, which Java writes in format version 1 since 0.12 and keeps next to an old snapshot's `manifests` on every metadata commit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT

# The table `location` is relative, so the table is addressed relative to the directory that contains `data`.
LEGACY='data/persistent/iceberg_v1_deprecated/default/legacy_v1'
SRC="$CUR_DIR/data_iceberg/duckdb/$LEGACY"

# Builds `$WORK/<name>/$LEGACY` whose only metadata file is `v3.metadata.json`: `v2.metadata.json` with the current fields,
# in format version <version>. The snapshot is kept as it is (`manifests`, no `sequence-number`), as an upgrade to version 2 keeps it.
make_copy()
{
    local dir="$WORK/$1/$LEGACY"
    mkdir -p "$dir/metadata"
    cp -r "$SRC/data" "$dir/"
    cp "$SRC/metadata/d65f86b0-b799-467f-b1f4-9c697e4c4fc7-m0.avro" "$dir/metadata/"
    python3 - "$SRC/metadata/v2.metadata.json" "$dir/metadata/v3.metadata.json" "$2" <<'EOF'
import json, sys
m = json.load(open(sys.argv[1]))
m["current-schema-id"] = 0
m["schemas"] = [m["schema"]]
m["default-spec-id"] = 0
m["partition-specs"] = [{"spec-id": 0, "fields": m["partition-spec"]}]
m["default-sort-order-id"] = 0
m["sort-orders"] = [{"order-id": 0, "fields": []}]
if sys.argv[3] == "2":
    m["format-version"] = 2
    m["last-sequence-number"] = 0
json.dump(m, open(sys.argv[2], "w"), indent=4)
EOF
    find "$WORK/$1" -exec touch -d '2001-01-01 00:00:00' {} +
}

# Runs clickhouse-local in `$WORK/<name>`.
local_in()
{
    local name=$1
    shift
    (cd "$WORK/$name" && $CLICKHOUSE_LOCAL --session_timezone UTC "$@" < /dev/null)
}

# Prints `ok` on success, else the error code name of the first exception.
error_code()
{
    local out
    if out=$(local_in "$@" 2>&1); then
        echo ok
    else
        grep -m1 -oE '^Code: [0-9]+\. .*\(([A-Z_]+)\)' <<<"$out" | grep -oE '\([A-Z_]+\)$' | tr -d '()'
    fi
}

metadata_files()
{
    ls "$WORK/$1/$LEGACY/metadata" | grep -c 'metadata.json$'
}

make_copy v1 1
make_copy v2 2

echo '--- format version 1'
local_in v1 -q "SELECT * FROM icebergLocal('$LEGACY') ORDER BY id"
local_in v1 -q "SELECT id, amount FROM icebergLocal('$LEGACY') WHERE category = 'beta'"
local_in v1 -q "
    CREATE TABLE t ENGINE = IcebergLocal('$LEGACY');
    SELECT snapshot_id, parent_id, is_current_ancestor FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't'"
# Writing on top of such a snapshot is refused before anything is committed.
error_code v1 --allow_insert_into_iceberg=1 -q "
    CREATE TABLE t ENGINE = IcebergLocal('$LEGACY');
    INSERT INTO t VALUES (4, 'gamma', 40)"
local_in v1 -q "SELECT count(), sum(id), sum(amount) FROM icebergLocal('$LEGACY')"
metadata_files v1

echo '--- upgraded to format version 2'
local_in v2 -q "SELECT * FROM icebergLocal('$LEGACY') ORDER BY id"
# The embedded manifest and its data files are reachable, so nothing is an orphan.
local_in v2 --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 -q "
    CREATE TABLE t ENGINE = IcebergLocal('$LEGACY');
    ALTER TABLE t EXECUTE remove_orphan_files(older_than = '2002-01-01 00:00:00', dry_run = 1)"
error_code v2 --allow_experimental_iceberg_compaction=1 -q "
    CREATE TABLE t ENGINE = IcebergLocal('$LEGACY');
    OPTIMIZE TABLE t"
error_code v2 --allow_experimental_iceberg_compaction=1 -q "
    CREATE TABLE t ENGINE = IcebergLocal('$LEGACY');
    OPTIMIZE TABLE t MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1"
metadata_files v2
