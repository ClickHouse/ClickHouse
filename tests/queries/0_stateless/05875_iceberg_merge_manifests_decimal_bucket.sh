#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# The `bucket` transform of a decimal column returns an Iceberg `int`, so the merge of manifests on INSERT writes its
# partition values as an integer: the precision of the decimal source only matters for the transforms that return
# the decimal itself (`identity`, `truncate`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${T}"

trap 'rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${T} (d Decimal(5, 2), v Int32) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') PARTITION BY (icebergBucket(4, d))"
# Insert 3 merges the manifests of inserts 1 and 2 into one.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_manifest_min_count_to_compact=2 -m --query "
    INSERT INTO ${T} VALUES (1.25, 10);
    INSERT INTO ${T} VALUES (1.25, 20);
    INSERT INTO ${T} VALUES (1.25, 30);"

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

# Two manifests are left: the merged one and the one of insert 3.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${MANIFEST_LIST}', 'Avro')"

# The partition summary of the merged manifest has 4-byte bounds of an Iceberg `int`.
${CLICKHOUSE_CLIENT} --query "
    SELECT existing_files_count, length(partitions[1].lower_bound), length(partitions[1].upper_bound)
    FROM file('${MANIFEST_LIST}', 'Avro')
    WHERE existing_files_count > 0"

MERGED_MANIFEST=$(${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(manifest_path, 'file://', '') FROM file('${MANIFEST_LIST}', 'Avro') WHERE existing_files_count > 0")

# The merged manifest declares the bucket partition field as `int`.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), groupUniqArray(toTypeName(assumeNotNull(tupleElement(data_file.partition, 1))))
    FROM file('${MERGED_MANIFEST}', 'Avro')"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${T}"
${CLICKHOUSE_CLIENT} --query "SELECT d, v FROM ${T} WHERE d = 1.25 ORDER BY v"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${T}"
