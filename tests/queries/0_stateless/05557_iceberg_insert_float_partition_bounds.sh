#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One manifest for all partitions of an INSERT, so the summary aggregates several partition values.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --iceberg_insert_single_manifest_for_all_partitions=1"

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
ROOT="${USER_FILES_PATH}/${PREFIX}"
CREATED_TABLES=()

cleanup()
{
    for table in "${CREATED_TABLES[@]}"; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table}"
    done
    rm -rf "${ROOT}"
}
trap cleanup EXIT

# Creates a table partitioned by `f`, inserts one row per partition and prints the manifest-list
# summary of `f`.
run_case()
{
    local suffix="$1" float_type="$2" reinterpret_fn="$3" values="$4"
    local table="${PREFIX}_${suffix}"
    CREATED_TABLES+=("${table}")

    echo "--- ${suffix}: ${float_type}, values=${values}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (f ${float_type})
        ENGINE = IcebergLocal('${ROOT}/${suffix}/')
        PARTITION BY f
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
        "INSERT INTO ${table} VALUES ${values}"

    local manifest_list
    manifest_list=$(find "${ROOT}/${suffix}/metadata" -maxdepth 1 -name 'snap-*.avro' -type f \
        -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-)

    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            'contains_null=' || toString(tupleElement(partitions[1], 'contains_null')),
            'contains_nan=' || toString(tupleElement(partitions[1], 'contains_nan')),
            'lower_bound=' || toString(${reinterpret_fn}(tupleElement(partitions[1], 'lower_bound'))),
            'upper_bound=' || toString(${reinterpret_fn}(tupleElement(partitions[1], 'upper_bound')))
        FROM file('${manifest_list}', Avro)
    "
}

# Case A/B: a NaN partition sets `contains_nan` but is excluded from the bounds.
run_case A Float32 reinterpretAsFloat32 '(1.5), (3.5), (nan)'
run_case B Float64 reinterpretAsFloat64 '(1.5), (3.5), (nan)'

# Case C/D: no NaN partition: `contains_nan` stays `NULL`, the bounds are the plain min/max.
run_case C Float32 reinterpretAsFloat32 '(1.5), (3.5)'
run_case D Float64 reinterpretAsFloat64 '(1.5), (3.5)'

# Case E/F: -0.0 sorts before +0.0 (spec: "Bounds for Float and Double").
run_case E Float32 reinterpretAsFloat32 '(0.0), (-0.0), (5.5), (nan)'
run_case F Float64 reinterpretAsFloat64 '(0.0), (-0.0), (5.5), (nan)'

# Case G/H: only the two zeros. `ManifestPartitionSummary::add` sees the partitions in hash order;
# if equal zeros kept the first one, both bounds would be the same zero, wrong in either order.
run_case G Float32 reinterpretAsFloat32 '(0.0), (-0.0)'
run_case H Float64 reinterpretAsFloat64 '(0.0), (-0.0)'
