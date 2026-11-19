#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# The merge of manifests on INSERT writes the partition values with the physical types of the Iceberg partition
# types (the `bucket` transform returns `UInt32`, but its Iceberg type is `int`), and keeps `split_offsets`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${T}"

trap 'rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T} (k Int32, v Int32) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') PARTITION BY (icebergBucket(4, k))"
# Insert 3 merges the manifests of inserts 1 and 2 into one.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_manifest_min_count_to_compact=2 -m --query "
    INSERT INTO ${T} VALUES (1, 10);
    INSERT INTO ${T} VALUES (2, 20);
    INSERT INTO ${T} VALUES (3, 30);"

MANIFEST_LIST=$(python3 - "${TABLE_PATH}/metadata" <<'PY'
import glob, json, os, re, sys
directory = sys.argv[1]
latest = max(glob.glob(os.path.join(directory, "*.metadata.json")), key=lambda p: int(re.search(r"v(\d+)[^/]*\.metadata\.json$", p).group(1)))
meta = json.load(open(latest))
snapshot = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
path = snapshot["manifest-list"]
if path.startswith("file://"):
    path = path[len("file://"):]
print(path)
PY
)

# The partition summary of the merged manifest has 4-byte bounds of an Iceberg `int`.
${CLICKHOUSE_CLIENT} --query "
    SELECT existing_files_count, length(partitions[1].lower_bound), length(partitions[1].upper_bound)
    FROM file('${MANIFEST_LIST}', 'Avro')
    WHERE existing_files_count > 0"

MERGED_MANIFEST=$(${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(manifest_path, 'file://', '') FROM file('${MANIFEST_LIST}', 'Avro') WHERE existing_files_count > 0")

# The merged manifest declares the bucket partition field as `int`, and its entries keep their `split_offsets`.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count(),
        groupUniqArray(toTypeName(assumeNotNull(tupleElement(data_file.partition, 1)))),
        min(length(data_file.split_offsets)) > 0
    FROM file('${MERGED_MANIFEST}', 'Avro')"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T}"
${CLICKHOUSE_CLIENT} --query "SELECT k, v FROM ${T} WHERE k = 2"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${T}"
