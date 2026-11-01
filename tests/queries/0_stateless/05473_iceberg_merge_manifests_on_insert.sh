#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An INSERT into an Iceberg table merges the small manifests of the parent snapshot within the same commit,
# so the number of manifests a query has to read does not grow with the number of inserts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

trap 'rm -rf "${USER_FILES_PATH}/${PREFIX}_"*' EXIT

# Prints a field of the latest metadata file of the table: `manifest_list` of the current snapshot,
# or `snapshot <sequence number>` for the id of the snapshot with that sequence number.
metadata()
{
    python3 - "${USER_FILES_PATH}/$1/metadata" "${@:2}" <<'PY'
import glob, json, os, re, sys
directory = sys.argv[1]
latest = max(glob.glob(os.path.join(directory, "*.metadata.json")), key=lambda p: int(re.search(r"v(\d+)[^/]*\.metadata\.json$", p).group(1)))
meta = json.load(open(latest))
if sys.argv[2] == "manifest_list":
    snapshot = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
    path = snapshot["manifest-list"]
    if path.startswith("file://"):
        path = path[len("file://"):]
    print(path)
elif sys.argv[2] == "snapshot":
    print(next(s["snapshot-id"] for s in meta["snapshots"] if s["sequence-number"] == int(sys.argv[3])))
elif sys.argv[2] == "set_property":
    meta.setdefault("properties", {})[sys.argv[3]] = sys.argv[4]
    json.dump(meta, open(latest, "w"))
PY
}

# Number of manifests in the current manifest list, and the added and existing data files they list.
manifests()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), sum(added_files_count), sum(existing_files_count)
        FROM file('$(metadata "$1" manifest_list)', 'Avro')"
}

inserts()
{
    for i in $(seq "$2" "$3"); do echo "INSERT INTO $1 VALUES ($((i % 10)), ${i});"; done
}

echo "--- partitioned, threshold 10"
T="${PREFIX}_partitioned"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T} (p Int32, v Int32) ENGINE = IcebergLocal('${USER_FILES_PATH}/${T}/', 'Parquet') PARTITION BY (p)"
# Inserts 11, 20 and 29 merge the 10 small manifests of their parent into one; the merged manifest of the
# previous merge is small too, so it is merged again. In the end: the merged manifest with the files
# of 28 inserts, and the manifests of inserts 29 and 30.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_manifest_min_count_to_compact=10 -m --query "$(inserts "${T}" 1 30)"
manifests "${T}"
# The merge does not create snapshots of its own.
${CLICKHOUSE_CLIENT} --query "SELECT count(), groupUniqArray(operation) FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${T}'"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T}"
# The manifest-list partition bounds of a merged manifest cover all its partitions, so pruning keeps the right files.
${CLICKHOUSE_CLIENT} --query "SELECT p, count(), sum(v) FROM ${T} WHERE p IN (0, 3, 9) GROUP BY p ORDER BY p"
# Snapshots before a merge still see their own manifests.
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T} SETTINGS iceberg_snapshot_id = $(metadata "${T}" snapshot 15)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T} SETTINGS iceberg_snapshot_id = $(metadata "${T}" snapshot 25)"
# `OPTIMIZE TABLE ... MANIFEST` writes one manifest per partition, so it cannot reduce the count of manifests spanning partitions.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${T} MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 2"
manifests "${T}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T}"

echo "--- unpartitioned, threshold 3"
T="${PREFIX}_unpartitioned"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T} (p Int32, v Int32) ENGINE = IcebergLocal('${USER_FILES_PATH}/${T}/', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_manifest_min_count_to_compact=3 -m --query "$(inserts "${T}" 1 5)"
manifests "${T}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T}"

echo "--- table properties"
T="${PREFIX}_disabled"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T} (p Int32, v Int32) ENGINE = IcebergLocal('${USER_FILES_PATH}/${T}/', 'Parquet') PARTITION BY (p)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "$(inserts "${T}" 1 1)"
metadata "${T}" set_property commit.manifest-merge.enabled false
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --iceberg_manifest_min_count_to_compact=2 -m --query "$(inserts "${T}" 2 6)"
manifests "${T}"

T="${PREFIX}_min_count"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T} (p Int32, v Int32) ENGINE = IcebergLocal('${USER_FILES_PATH}/${T}/', 'Parquet') PARTITION BY (p)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "$(inserts "${T}" 1 1)"
# The table property takes precedence over the setting.
metadata "${T}" set_property commit.manifest.min-count-to-merge 2
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 -m --query "$(inserts "${T}" 2 4)"
manifests "${T}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T}"

for suffix in partitioned unpartitioned disabled min_count; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${PREFIX}_${suffix}"
done
