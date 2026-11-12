#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/125555
# The delete totals of a snapshot summary are optional. A commit on top of a snapshot that omits them
# must keep them absent, otherwise the trivial count() trusts `total-records` and counts deleted rows.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_125555_${RANDOM}"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
mkdir -p "${ROOT}"

# `strip` removes the delete totals from the current snapshot's summary, `report` prints them.
metadata() {
    python3 - "$1" "$2" <<'PY'
import json, os, re, sys
directory, mode = sys.argv[1], sys.argv[2]
fields = ("total-delete-files", "total-position-deletes", "total-equality-deletes")

def order(name):
    version = re.match(r"v?0*(\d+)", name)
    return (json.load(open(os.path.join(directory, name))).get("last-updated-ms", 0),
            int(version.group(1)) if version else 0)

names = [name for name in os.listdir(directory) if name.endswith(".metadata.json")]
path = os.path.join(directory, max(names, key=order))
meta = json.load(open(path))
summary = {s["snapshot-id"]: s for s in meta["snapshots"]}[meta["current-snapshot-id"]]["summary"]

if mode == "strip":
    for field in fields:
        assert field in summary, "the fixture never had %s to remove" % field
        del summary[field]
    json.dump(meta, open(path, "w"))
else:
    print("delete totals: " + " ".join("%s=%s" % (field, summary.get(field, "absent")) for field in fields))
PY
}

counts() {
    for trivial in 1 0; do
        ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --optimize_trivial_count_query=${trivial} \
            --query "SELECT count() FROM $1"
    done
}

# Two rows in one partition, one of them removed by a position delete, then the delete totals are
# stripped from the current snapshot.
prepare() {
    local table=$1 path=$2
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (a Int32, v Int32) ENGINE = IcebergLocal('${path}', 'Parquet') PARTITION BY (a)"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "
        INSERT INTO ${table} VALUES (1, 10);
        INSERT INTO ${table} VALUES (1, 20);
        ALTER TABLE ${table} DELETE WHERE v = 10;
    "
    metadata "${path}metadata" strip
    # An attached table keeps the metadata version it already parsed, so reload it to pick up the edit.
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "DETACH TABLE ${table}"
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --send_logs_level=fatal --query "ATTACH TABLE ${table}"
    counts "${table}"
}

run_case() {
    local name=$1 query=$2
    local table="t_${CLICKHOUSE_DATABASE}_${name}"
    local path="${ROOT}/${name}/"
    prepare "${table}" "${path}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_experimental_iceberg_compaction=1 \
        --use_iceberg_metadata_files_cache=0 --query "${query//TABLE_NAME/${table}}"
    counts "${table}"
    metadata "${path}metadata" report
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table} SYNC"
}

echo "--- INSERT"
run_case insert "INSERT INTO TABLE_NAME VALUES (1, 30)"
echo "--- OPTIMIZE MANIFEST"
run_case optimize_manifest "OPTIMIZE TABLE TABLE_NAME MANIFEST SETTINGS iceberg_manifest_min_count_to_compact=1"
echo "--- ALTER DELETE"
run_case alter_delete "ALTER TABLE TABLE_NAME DELETE WHERE v = 20"
