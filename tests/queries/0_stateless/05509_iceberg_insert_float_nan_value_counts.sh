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

# Creates a three-column `(f32 Nullable(Float32), i Nullable(Int32), f64 Nullable(Float64))`
# table (the non-float `i` sits between the two float columns, so it gets a field id, 2, sandwiched
# between f32's 1 and f64's 3 - a stray off-by-one in the NaN-capable filtering would show up here).
# Inserts `values` for f32/f64 only (`i` stays NULL in every row: a non-float column must never get
# a `nan_value_counts` entry), and prints both whole maps as (field_id, count) pairs sorted by
# field_id - `i`'s id (2) must never appear in `nan_counts`, only ever in `null_counts`.
run_case()
{
    local suffix="$1" values="$2" max_block_size="${3:-}"
    local table="${PREFIX}_${suffix}"
    CREATED_TABLES+=("${table}")

    local settings_clause=""
    [ -n "${max_block_size}" ] && settings_clause="SETTINGS max_block_size = ${max_block_size}"

    echo "--- ${suffix}: values=${values}${settings_clause:+ (${settings_clause})}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (f32 Nullable(Float32), i Nullable(Int32), f64 Nullable(Float64))
        ENGINE = IcebergLocal('${ROOT}/${suffix}/')
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query \
        "INSERT INTO ${table} (f32, f64) ${settings_clause} VALUES ${values}"

    for manifest in $(find "${ROOT}/${suffix}/metadata" -maxdepth 1 -name '*.avro' \
            -not -name 'snap-*.avro' -type f | sort); do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT
                'null_counts=' || toString(arraySort(tupleElement(data_file, 'null_value_counts'))),
                'nan_counts=' || toString(arraySort(tupleElement(data_file, 'nan_value_counts')))
            FROM file('${manifest}', Avro)
            ORDER BY 1, 2
        "
    done
}

# Case A: the whole column is NaN, so nan_count equals the row count and null_count stays 0.
run_case A '(nan, nan)'

# Case B: no NaN, nan_count is 0 (still present in the map, since f32/f64 are NaN-capable types).
run_case B '(1.5, 1.5), (3.5, 3.5)'

# Case C: NaN mixed with real values, nan_count must reflect only the NaN rows.
run_case C '(1.5, 1.5), (nan, nan), (-5.5, -5.5)'

# Case D: same values as C, but with a whole-NaN chunk (`max_block_size = 1`) merged separately;
# the count must still accumulate correctly across chunks, not just within one.
run_case D '(1.5, 1.5), (nan, nan), (-5.5, -5.5)' 1

# Case E: NULL mixed with real values, null_count must reflect NULL and nan_count must stay 0
# (a NULL row's nested value must not also be tallied as NaN).
run_case E '(1.5, 1.5), (NULL, NULL), (-5.5, -5.5)'

# Case F: NULL and NaN both present, null_count and nan_count must count them separately
# (a NULL row's nested value must not also be tallied as NaN, or vice versa).
run_case F '(1.5, 1.5), (NULL, NULL), (nan, nan), (-5.5, -5.5)'
