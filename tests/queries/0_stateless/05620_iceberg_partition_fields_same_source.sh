#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
TABLE_TIME="time_${CLICKHOUSE_DATABASE}"
TABLE_IDENTITY="identity_${CLICKHOUSE_DATABASE}"
TABLE_PARAMETERIZED="parameterized_${CLICKHOUSE_DATABASE}"
TABLE_LEGACY_SINGLE="legacy_single_${CLICKHOUSE_DATABASE}"
TABLE_LEGACY="legacy_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${TABLE_TIME};
        DROP TABLE IF EXISTS ${TABLE_IDENTITY};
        DROP TABLE IF EXISTS ${TABLE_PARAMETERIZED};
        DROP TABLE IF EXISTS ${TABLE_LEGACY_SINGLE};
        DROP TABLE IF EXISTS ${TABLE_LEGACY};
        DROP TABLE IF EXISTS invalid_duplicate_${CLICKHOUSE_DATABASE};
        DROP TABLE IF EXISTS invalid_collision_${CLICKHOUSE_DATABASE}" >/dev/null 2>&1
    rm -rf "${BASE_DIR}"
}
cleanup
trap cleanup EXIT

query()
{
    ${CLICKHOUSE_CLIENT} \
        --allow_insert_into_iceberg=1 \
        --mutations_sync=2 \
        --query "$1"
}

check_partition_names()
{
    local label=$1
    local table_dir=$2
    python3 - "${label}" "${table_dir}" <<'EOF'
import glob
import json
import os
import re
import sys

label, table_dir = sys.argv[1:]
metadata_files = glob.glob(os.path.join(table_dir, "metadata", "v*.metadata.json"))
latest = max(metadata_files, key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
metadata = json.load(open(latest))
spec_id = metadata["default-spec-id"]
spec = next(spec for spec in metadata["partition-specs"] if spec["spec-id"] == spec_id)
print(label, ",".join(field["name"] for field in spec["fields"]))
EOF
}

optimize()
{
    local table=$1
    local error
    error=$(${CLICKHOUSE_CLIENT} \
        --allow_experimental_iceberg_compaction=1 \
        --query "OPTIMIZE TABLE ${table}" 2>&1)
    local is_cloud
    is_cloud=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
    if [[ "${is_cloud}" != "1" && -n "${error}" ]]; then
        echo "OPTIMIZE failed: ${error}"
    fi
}

optimize_manifest()
{
    local table=$1
    local error
    error=$(${CLICKHOUSE_CLIENT} \
        --allow_experimental_iceberg_compaction=1 \
        --iceberg_manifest_min_count_to_compact=1 \
        --query "OPTIMIZE TABLE ${table} MANIFEST" 2>&1)
    local is_cloud
    is_cloud=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
    if [[ "${is_cloud}" != "1" && -n "${error}" ]]; then
        echo "OPTIMIZE MANIFEST failed: ${error}"
    fi
}

echo '--- year and hour'
query "
    CREATE TABLE ${TABLE_TIME} (ts DateTime64(6, 'UTC'), i Int64)
    ENGINE = IcebergLocal('${BASE_DIR}/time', 'Parquet')
    PARTITION BY (icebergYear(ts), icebergHour(ts));
    INSERT INTO ${TABLE_TIME} VALUES (toDateTime64('2024-01-01 05:00:00', 6, 'UTC'), 1);
    INSERT INTO ${TABLE_TIME} VALUES (toDateTime64('2023-01-01 05:00:00', 6, 'UTC'), 2);
    INSERT INTO ${TABLE_TIME} VALUES (toDateTime64('2024-01-01 05:00:00', 6, 'UTC'), 3)"
check_partition_names time "${BASE_DIR}/time"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = '${TABLE_TIME}' AND content = 'DATA'"
optimize "${TABLE_TIME}"
query "ALTER TABLE ${TABLE_TIME} UPDATE i = 10 WHERE i = 1"
${CLICKHOUSE_CLIENT} --query "SELECT i, icebergYear(ts), icebergHour(ts) FROM ${TABLE_TIME} ORDER BY i"

echo '--- identity and day'
query "
    CREATE TABLE ${TABLE_IDENTITY} (ts DateTime64(6, 'UTC'), i Int64)
    ENGINE = IcebergLocal('${BASE_DIR}/identity', 'Parquet')
    PARTITION BY (ts, icebergDay(ts));
    INSERT INTO ${TABLE_IDENTITY} VALUES
        (toDateTime64('2024-01-02 05:00:00', 6, 'UTC'), 1),
        (toDateTime64('2024-01-02 05:00:00', 6, 'UTC'), 2)"
check_partition_names identity "${BASE_DIR}/identity"
query "ALTER TABLE ${TABLE_IDENTITY} DELETE WHERE i = 1"
optimize "${TABLE_IDENTITY}"
${CLICKHOUSE_CLIENT} --query "SELECT i, icebergDay(ts) FROM ${TABLE_IDENTITY} ORDER BY i"

echo '--- parameterized transforms'
query "
    CREATE TABLE ${TABLE_PARAMETERIZED} (x Int64, s String)
    ENGINE = IcebergLocal('${BASE_DIR}/parameterized', 'Parquet')
    PARTITION BY (
        icebergBucket(4, x),
        icebergBucket(8, x),
        icebergTruncate(2, s),
        icebergTruncate(3, s));
    INSERT INTO ${TABLE_PARAMETERIZED} VALUES (1, 'abcd');
    INSERT INTO ${TABLE_PARAMETERIZED} VALUES (2, 'efgh')"
check_partition_names parameterized "${BASE_DIR}/parameterized"
optimize_manifest "${TABLE_PARAMETERIZED}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PARAMETERIZED}"

echo '--- single transform'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE ${TABLE_LEGACY_SINGLE} (ts DateTime64(6, 'UTC'), i Int64)
    ENGINE = IcebergLocal('${BASE_DIR}/legacy_single', 'Parquet')
    PARTITION BY icebergYear(ts);
    INSERT INTO ${TABLE_LEGACY_SINGLE} VALUES (toDateTime64('2024-01-01 05:00:00', 6, 'UTC'), 1);
    INSERT INTO ${TABLE_LEGACY_SINGLE} VALUES (toDateTime64('2024-02-01 05:00:00', 6, 'UTC'), 2)"
check_partition_names legacy_single "${BASE_DIR}/legacy_single"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
    ALTER TABLE ${TABLE_LEGACY_SINGLE} DELETE WHERE i = 1"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE_LEGACY_SINGLE}"
${CLICKHOUSE_CLIENT} --query "SELECT i, icebergYear(ts) FROM ${TABLE_LEGACY_SINGLE} ORDER BY i"

echo '--- invalid names'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE invalid_duplicate_${CLICKHOUSE_DATABASE} (ts DateTime64(6, 'UTC'))
    ENGINE = IcebergLocal('${BASE_DIR}/invalid_duplicate', 'Parquet')
    PARTITION BY (icebergYear(ts), icebergYear(ts))" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE invalid_collision_${CLICKHOUSE_DATABASE} (ts DateTime64(6, 'UTC'), ts_year Int64)
    ENGINE = IcebergLocal('${BASE_DIR}/invalid_collision', 'Parquet')
    PARTITION BY icebergYear(ts)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '--- legacy duplicate names'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_LEGACY} (ts DateTime64(6, 'UTC'), i Int64)
    ENGINE = IcebergLocal('${BASE_DIR}/legacy', 'Parquet')
    PARTITION BY (icebergYear(ts), icebergHour(ts));
    DROP TABLE ${TABLE_LEGACY}"
python3 - "${BASE_DIR}/legacy/metadata/v1.metadata.json" <<'EOF'
import json
import sys

path = sys.argv[1]
with open(path) as metadata_file:
    metadata = json.load(metadata_file)
for field in metadata["partition-specs"][0]["fields"]:
    field["name"] = "ts"
with open(path, "w") as metadata_file:
    json.dump(metadata, metadata_file)
EOF
${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --use_iceberg_metadata_files_cache=0 --query "
    CREATE TABLE ${TABLE_LEGACY} ENGINE = IcebergLocal('${BASE_DIR}/legacy', 'Parquet')" >/dev/null
${CLICKHOUSE_CLIENT} \
    --allow_insert_into_iceberg=1 \
    --use_iceberg_metadata_files_cache=0 \
    --query "
    INSERT INTO ${TABLE_LEGACY} VALUES (toDateTime64('2024-01-01 05:00:00', 6, 'UTC'), 1)" 2>&1 \
    | grep -o -m1 "partition field name 'ts' is used more than once"
python3 - "${BASE_DIR}/legacy/data" <<'EOF'
import glob
import os
import sys

print(sum(os.path.isfile(path) for path in glob.glob(os.path.join(sys.argv[1], "**"), recursive=True)))
EOF
