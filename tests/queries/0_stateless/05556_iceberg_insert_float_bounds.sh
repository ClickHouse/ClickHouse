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

# Creates a `(f32, f64)` table, `Nullable` unless `f32_type`/`f64_type` are given, inserts the
# `(f32, f64)` pairs of `values` and prints the bounds as (field id, value): 1 is f32, 2 is f64.
# `split` puts each row in its own chunk, so the bounds are merged by `uniteRanges`
# (`INSERT ... VALUES` is always one chunk). Parquet is pinned: with a format that prefers large
# blocks, such as ORC, insert squashing would merge the chunks back.
run_case()
{
    local suffix="$1" values="$2" split="${3:-}"
    local f32_type="${4:-Nullable(Float32)}" f64_type="${5:-Nullable(Float64)}"
    local table="${PREFIX}_${suffix}"
    CREATED_TABLES+=("${table}")

    echo "--- ${suffix}: values=${values}${split:+ (one row per chunk)}${4:+ (${f32_type}, ${f64_type})}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (f32 ${f32_type}, f64 ${f64_type})
        ENGINE = IcebergLocal('${ROOT}/${suffix}/', 'Parquet')
    "
    if [ -n "${split}" ]; then
        local num_rows
        num_rows=$(grep -o '(' <<< "${values}" | wc -l)
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
            INSERT INTO ${table}
            WITH [${values}] AS rows
            SELECT rows[number + 1].1, rows[number + 1].2 FROM numbers(${num_rows})
            SETTINGS max_block_size = 1, max_threads = 1"
    else
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} VALUES ${values}"
    fi

    local manifest
    manifest=$(find "${ROOT}/${suffix}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro')
    ${CLICKHOUSE_CLIENT} --query "
        WITH bounds -> arraySort(arrayMap(x -> (x.1, if(x.1 = 1, reinterpretAsFloat32(x.2), reinterpretAsFloat64(x.2))), bounds)) AS decode
        SELECT
            'lower=' || toString(decode(tupleElement(data_file, 'lower_bounds'))),
            'upper=' || toString(decode(tupleElement(data_file, 'upper_bounds')))
        FROM file('${manifest}', Avro)"
}

# Case A: only NaN: no bound.
run_case A '(nan, nan)'

# Case B: no NaN: the plain min/max.
run_case B '(1.5, 1.5), (3.5, 3.5)'

# Case C: NaN among real values is excluded from the bounds.
run_case C '(1.5, 1.5), (nan, nan), (-5.5, -5.5)'

# Case D: like C, one row per chunk: the all-NaN chunk must not win a bound in `uniteRanges`.
run_case D '(1.5, 1.5), (nan, nan), (-5.5, -5.5)' split

# Case E: `NULL` among real values is excluded from the bounds.
run_case E '(1.5, 1.5), (NULL, NULL), (-5.5, -5.5)'

# Case F: -0.0 sorts before +0.0 (spec: "Bounds for Float and Double"), within one chunk.
run_case F '(nan, nan), (0.0, 0.0), (-0.0, -0.0), (5.5, 5.5)'

# Case G: like F, one row per chunk: the zeros are merged by `uniteRanges`.
run_case G '(nan, nan), (0.0, 0.0), (-0.0, -0.0)' split

# Case H: G with the zeros swapped. Together G and H catch any tie-break by row order: it gets one of
# the bounds wrong in one of them.
run_case H '(nan, nan), (-0.0, -0.0), (0.0, 0.0)' split

# Case I: like F with non-`Nullable` columns.
run_case I '(nan, nan), (0.0, 0.0), (-0.0, -0.0), (2.5, 2.5)' '' Float32 Float64

# Case J: infinities are valid bounds.
run_case J '(inf, inf), (-inf, -inf), (1.0, 1.0), (nan, nan)'
