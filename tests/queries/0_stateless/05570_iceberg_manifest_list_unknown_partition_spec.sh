#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)
# - no-replicated-database: IcebergLocal is non-replicated

# Every manifest list entry names the partition spec its manifest was written with (`partition_spec_id`),
# which must be one of the `partition-specs` of the table metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_V1="t1_${CLICKHOUSE_DATABASE}"
TABLE_V2="t2_${CLICKHOUSE_DATABASE}"
TABLE_DIR="${USER_FILES_PATH}/${TABLE}"
TABLE_V1_DIR="${USER_FILES_PATH}/${TABLE_V1}"
TABLE_V2_DIR="${USER_FILES_PATH}/${TABLE_V2}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; DROP TABLE IF EXISTS ${TABLE_V1}; DROP TABLE IF EXISTS ${TABLE_V2}"
    rm -rf "${TABLE_DIR}" "${TABLE_V1_DIR}" "${TABLE_V2_DIR}"
}
cleanup
trap cleanup EXIT

query()
{
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 --query "$1"
}

# Prints `rejected` when the query fails with ICEBERG_SPECIFICATION_VIOLATION naming the partition spec $2.
rejected()
{
    local error
    error=$(query "$1" 2>&1)
    if grep -q -F 'ICEBERG_SPECIFICATION_VIOLATION' <<< "$error" && grep -q -F "refers to the partition spec $2," <<< "$error"; then
        echo 'rejected'
    else
        echo 'not rejected'
    fi
}

# Publishes the next metadata version of the table in $1, changed by the Python statements in $2 on `m`.
publish()
{
    python3 - "$1/metadata" "$2" <<'EOF'
import json, os, re, sys
metadata_dir, change = sys.argv[1], sys.argv[2]
version = max(int(re.fullmatch(r"v(\d+)\.metadata\.json", f).group(1)) for f in os.listdir(metadata_dir) if re.fullmatch(r"v(\d+)\.metadata\.json", f))
with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
    m = json.load(f)
exec(change)
m["last-updated-ms"] = m.get("last-updated-ms", 0) + 60000
with open(os.path.join(metadata_dir, f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(m, f)
EOF
}

# The data is written under the partition spec -1, so the manifest list refers to it.
query "CREATE TABLE ${TABLE} (id Int32, p Int32) ENGINE = IcebergLocal('${TABLE_DIR}/') PARTITION BY p"
publish "${TABLE_DIR}" 'm["partition-specs"][0]["spec-id"] = -1; m["default-spec-id"] = -1'
query "INSERT INTO ${TABLE} VALUES (1, 10), (2, 20), (3, 10)"
echo "spec_-1 $(query "SELECT count(), sum(id) FROM ${TABLE}")"

# Partition evolution: the current spec is 1, the manifests keep -1.
publish "${TABLE_DIR}" 'm["partition-specs"].append({"spec-id": 1, "fields": []}); m["default-spec-id"] = 1'
echo "evolved $(query "SELECT count(), sum(id) FROM ${TABLE}")"

# The spec -1 is removed from the metadata, the manifest list still refers to it.
publish "${TABLE_DIR}" 'm["partition-specs"] = [s for s in m["partition-specs"] if s["spec-id"] != -1]'
# The runner may disable the optimizations that answer `count()` from the snapshot summary.
echo "removed count $(rejected "SELECT count() FROM ${TABLE} SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1" -1)"
echo "removed select $(rejected "SELECT * FROM ${TABLE}" -1)"

# Format version 1 metadata without `partition-specs` has only the spec 0 (`partition-spec`).
query "CREATE TABLE ${TABLE_V1} (id Int32) ENGINE = IcebergLocal('${TABLE_V1_DIR}/') SETTINGS iceberg_format_version = 1"
query "INSERT INTO ${TABLE_V1} VALUES (1), (2)"
publish "${TABLE_V1_DIR}" 'del m["partition-specs"]; del m["default-spec-id"]'
echo "v1_without_partition_specs $(query "SELECT count(), sum(id) FROM ${TABLE_V1}")"

# Without `partition-spec` either, format version 1 metadata has no spec.
publish "${TABLE_V1_DIR}" 'del m["partition-spec"]'
echo "v1_without_any_partition_spec $(rejected "SELECT count() FROM ${TABLE_V1}" 0)"

# A null `partition-spec` declares no spec either.
publish "${TABLE_V1_DIR}" 'm["partition-spec"] = None'
echo "v1_with_null_partition_spec $(rejected "SELECT count() FROM ${TABLE_V1}" 0)"

# Format version 2 requires `partition-specs`, so without it no spec is known.
query "CREATE TABLE ${TABLE_V2} (id Int32) ENGINE = IcebergLocal('${TABLE_V2_DIR}/')"
query "INSERT INTO ${TABLE_V2} VALUES (1), (2)"
publish "${TABLE_V2_DIR}" 'del m["partition-specs"]'
echo "v2_without_partition_specs $(rejected "SELECT count() FROM ${TABLE_V2}" 0)"
