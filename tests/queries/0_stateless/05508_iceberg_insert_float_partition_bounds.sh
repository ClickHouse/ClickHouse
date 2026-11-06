#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# All partitions of one INSERT must land in a single manifest, so the partition summary below
# actually aggregates across different partition values instead of each manifest trivially
# holding just one.
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

# Creates a `(f <float_type>)` table partitioned by `f` itself, inserts `values` (one row per
# partition), and prints the manifest-list `partitions[1]` summary for that one partition field:
# contains_null/contains_nan plus the decoded lower/upper bound.
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

# Case A: a NaN partition sets contains_nan but is excluded from lower/upper_bound.
run_case A Float32 reinterpretAsFloat32 '(1.5), (3.5), (nan)'
run_case B Float64 reinterpretAsFloat64 '(1.5), (3.5), (nan)'

# Case C: no NaN partition - contains_nan stays NULL (never set), bound is the plain min/max.
run_case C Float32 reinterpretAsFloat32 '(1.5), (3.5)'
run_case D Float64 reinterpretAsFloat64 '(1.5), (3.5)'

# Case E/F: Iceberg orders -0.0 before +0.0 (spec: "Bounds for Float and Double"), unlike IEEE754
# comparison, where they're equal. lower_bound must be -0.0, not +0.0, even though both occur.
# hex() proves the sign bit, since toString() may not render it.
run_case E Float32 reinterpretAsFloat32 '(0.0), (-0.0), (5.5), (nan)'
run_case F Float64 reinterpretAsFloat64 '(0.0), (-0.0), (5.5), (nan)'

# Case G/H: only the two zeros, so both bounds come from a -0.0/+0.0 tie: lower must be -0.0 and
# upper +0.0. The partitions are folded in hash order, not insertion order, but with a
# first-seen-wins tie-break one of the two bounds is wrong whichever zero comes first.
run_case G Float32 reinterpretAsFloat32 '(0.0), (-0.0)'
run_case H Float64 reinterpretAsFloat64 '(0.0), (-0.0)'

for suffix in E F G H; do
    manifest_list=$(find "${ROOT}/${suffix}/metadata" -maxdepth 1 -name 'snap-*.avro' -type f \
        -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-)
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            '${suffix}_lower_bound_hex=' || hex(tupleElement(partitions[1], 'lower_bound')),
            '${suffix}_upper_bound_hex=' || hex(tupleElement(partitions[1], 'upper_bound'))
        FROM file('${manifest_list}', Avro)
    "
done
