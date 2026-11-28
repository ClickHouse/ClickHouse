#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Manifest-list pruning after partition spec evolution, with `partition-specs` also holding specs that no manifest uses.
# The first manifest is written with spec 0 (`p`), the second with spec 1 (`q`); specs 2..9 bucket `v` and are unused.
# Each line prints the condition, the rows read without partition pruning (the truth), the rows read with
# manifest-list pruning, and the number of manifests skipped by manifest-list pruning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT
cleanup

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0"

$CH --query "
    CREATE TABLE ${TABLE} (p Int32, q Int32, v Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY p
    SETTINGS allow_experimental_iceberg_compaction = 0"
$CH --query "INSERT INTO ${TABLE} VALUES (1, 10, 1), (2, 20, 2)" < /dev/null

# Evolve the spec the way an external writer would: the newest `metadata.json` gets spec 1 as the default spec,
# and eight more specs that no manifest refers to.
LATEST=$(printf "%s\n" "${TABLE_PATH}metadata/"*.metadata.json | sort -V | tail -n1)
python3 - "${LATEST}" <<'PY'
import json, sys
path = sys.argv[1]
m = json.load(open(path))
fid = m["last-partition-id"]
specs = m["partition-specs"]
fid += 1
specs.append({"spec-id": 1, "fields": [{"source-id": 2, "field-id": fid, "name": "q", "transform": "identity"}]})
for spec_id in range(2, 10):
    fid += 1
    specs.append({"spec-id": spec_id, "fields": [{"source-id": 3, "field-id": fid, "name": "v_bucket_%d" % spec_id, "transform": "bucket[%d]" % spec_id}]})
m["default-spec-id"] = 1
m["last-partition-id"] = fid
json.dump(m, open(path, "w"))
PY

$CH --query "INSERT INTO ${TABLE} VALUES (3, 30, 3), (4, 40, 4)" < /dev/null

# The partition tuples of the data files: `p` for the first two, `q` for the last two.
$CH --query "
    SELECT partition FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = '${TABLE}' AND content = 'DATA'
    ORDER BY partition"

LINES=()
arm=0
run_arm()
{
    local condition=$1
    local truth pruned
    arm=$((arm + 1))
    truth=$($CH --query "
        SELECT arraySort(groupArray(v)) FROM ${TABLE} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 0")
    pruned=$($CH --query_id "${TABLE}_${arm}" --query "
        SELECT arraySort(groupArray(v)) FROM ${TABLE} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1")
    LINES+=("${arm}"$'\t'"${condition}"$'\t'"${truth}"$'\t'"${pruned}")
}

# Spec 0 manifest bounds `p` to [1, 2], spec 1 manifest bounds `q` to [30, 40].
run_arm "p = 1"
run_arm "p = 3"
run_arm "p = 7"
run_arm "q = 30"
run_arm "q = 10"
run_arm "q = 99"
run_arm "p = 7 AND q = 99"
run_arm "v = 3"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

declare -A PRUNED_MANIFESTS
while IFS=$'\t' read -r id manifests; do
    PRUNED_MANIFESTS[${id}]=${manifests}
done < <(${CLICKHOUSE_CLIENT} --query "
    SELECT substring(query_id, length('${TABLE}_') + 1), ProfileEvents['IcebergPartitionPrunedManifestFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(query_id, '${TABLE}_')
    SETTINGS enable_parallel_replicas = 0")

for line in "${LINES[@]}"; do
    id=${line%%$'\t'*}
    echo "${line#*$'\t'}"$'\t'"${PRUNED_MANIFESTS[${id}]:-missing}"
done
