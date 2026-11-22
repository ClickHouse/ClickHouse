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

# Creates a `(f32, i Nullable(Int32), f64)` table (floats `Nullable` unless `f32_type`/`f64_type` are
# given), inserts `values` into f32/f64 (`i` stays `NULL`) and prints `null_value_counts` and
# `nan_value_counts`. `i` (field id 2, between the floats) must never get a NaN count.
# With `split`, each row is its own chunk (see `iceberg_insert_float_bounds` for why `numbers` and Parquet are used).
run_case()
{
    local suffix="$1" values="$2" split="${3:-}"
    local f32_type="${4:-Nullable(Float32)}" f64_type="${5:-Nullable(Float64)}"
    local table="${PREFIX}_${suffix}"
    CREATED_TABLES+=("${table}")

    echo "--- ${suffix}: values=${values}${split:+ (one row per chunk)}${4:+ (${f32_type}, ${f64_type})}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (f32 ${f32_type}, i Nullable(Int32), f64 ${f64_type})
        ENGINE = IcebergLocal('${ROOT}/${suffix}/', 'Parquet')
    "
    if [ -n "${split}" ]; then
        local num_rows
        num_rows=$(grep -o '(' <<< "${values}" | wc -l)
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
            INSERT INTO ${table} (f32, f64)
            WITH [${values}] AS rows
            SELECT rows[number + 1].1, rows[number + 1].2 FROM numbers(${num_rows})
            SETTINGS max_block_size = 1, max_threads = 1"
    else
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} (f32, f64) VALUES ${values}"
    fi

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

# Case A: only NaN.
run_case A '(nan, nan)'

# Case B: no NaN: the count is 0, but still written for a float column.
run_case B '(1.5, 1.5), (3.5, 3.5)'

# Case C: NaN among real values.
run_case C '(1.5, 1.5), (nan, nan), (-5.5, -5.5)'

# Case D: like C, one row per chunk: the counts are summed across chunks.
run_case D '(1.5, 1.5), (nan, nan), (-5.5, -5.5)' split

# Case E: a `NULL` row is not counted as NaN.
run_case E '(1.5, 1.5), (NULL, NULL), (-5.5, -5.5)'

# Case F: `NULL` and NaN together are counted separately.
run_case F '(1.5, 1.5), (NULL, NULL), (nan, nan), (-5.5, -5.5)'

# Case G: like C with non-`Nullable` columns.
run_case G '(1.5, 1.5), (nan, nan), (-5.5, -5.5)' '' Float32 Float64

# Case H: infinities are not NaN.
run_case H '(inf, inf), (-inf, -inf), (nan, nan)'
