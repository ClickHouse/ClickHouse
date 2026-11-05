#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

# Creates a two-column `(f32 Float32, f64 Float64)` table, inserts `values` (each row sets both
# columns to the same value, so one INSERT exercises both widths at once), and prints which field
# ids got bounds plus the decoded f32 (field id 1) / f64 (field id 2) bounds.
run_case()
{
    local suffix="$1" values="$2" max_block_size="${3:-}"
    local table="${PREFIX}_${suffix}"
    CREATED_TABLES+=("${table}")

    local settings_clause=""
    [ -n "${max_block_size}" ] && settings_clause="SETTINGS max_block_size = ${max_block_size}"

    echo "--- ${suffix}: values=${values}${settings_clause:+ (${settings_clause})}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (f32 Nullable(Float32), f64 Nullable(Float64))
        ENGINE = IcebergLocal('${ROOT}/${suffix}/')
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
        "INSERT INTO ${table} ${settings_clause} VALUES ${values}"

    for manifest in $(find "${ROOT}/${suffix}/metadata" -maxdepth 1 -name '*.avro' \
            -not -name 'snap-*.avro' -type f | sort); do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT
                'lower_ids=' || toString(arraySort(arrayMap(x -> x.1, tupleElement(data_file, 'lower_bounds')))),
                'upper_ids=' || toString(arraySort(arrayMap(x -> x.1, tupleElement(data_file, 'upper_bounds')))),
                'f32_lower=' || toString(arrayMap(x -> reinterpretAsFloat32(x.2),
                    arrayFilter(x -> x.1 = 1, tupleElement(data_file, 'lower_bounds')))),
                'f32_upper=' || toString(arrayMap(x -> reinterpretAsFloat32(x.2),
                    arrayFilter(x -> x.1 = 1, tupleElement(data_file, 'upper_bounds')))),
                'f64_lower=' || toString(arrayMap(x -> reinterpretAsFloat64(x.2),
                    arrayFilter(x -> x.1 = 2, tupleElement(data_file, 'lower_bounds')))),
                'f64_upper=' || toString(arrayMap(x -> reinterpretAsFloat64(x.2),
                    arrayFilter(x -> x.1 = 2, tupleElement(data_file, 'upper_bounds'))))
            FROM file('${manifest}', Avro)
            ORDER BY 1, 2, 3, 4
        "
    done
}

# Case A: the whole column is NaN for both widths, so the bound is unknown and must be omitted.
run_case A '(nan, nan)'

# Case B: no NaN in both columns, the bound is the plain min/max.
run_case B '(1.5, 1.5), (3.5, 3.5)'

# Case C: NaN mixed with real values, the bound must exclude NaN and reflect only the real ones.
run_case C '(1.5, 1.5), (nan, nan), (-5.5, -5.5)'

# Case D: same values as C, but with a whole NaN chunk (`max_block_size = 1`) unioned via
# `uniteRanges`, where NaN wins the upper bound.а it must still be omitted, not published as NaN.
run_case D '(1.5, 1.5), (nan, nan), (-5.5, -5.5)' 1

# Case E: NULL mixed with real values, the bound must exclude NULL and still be published.
run_case E '(1.5, 1.5), (NULL, NULL), (-5.5, -5.5)'

# Case F: Iceberg orders -0.0 before +0.0 (spec: "Bounds for Float and Double"), unlike IEEE754
# comparison, where they're equal. lower_bound must be -0.0, not +0.0, even though both occur.
# hex() proves the sign bit, since toString() may not render it.
run_case F '(nan, nan), (0.0, 0.0), (-0.0, -0.0), (5.5, 5.5)'

for manifest in $(find "${ROOT}/F/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | sort); do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            'f32_lower_hex=' || toString(arrayMap(x -> hex(x.2), arrayFilter(x -> x.1 = 1, tupleElement(data_file, 'lower_bounds')))),
            'f64_lower_hex=' || toString(arrayMap(x -> hex(x.2), arrayFilter(x -> x.1 = 2, tupleElement(data_file, 'lower_bounds'))))
        FROM file('${manifest}', Avro)
        ORDER BY 1, 2
    "
done
