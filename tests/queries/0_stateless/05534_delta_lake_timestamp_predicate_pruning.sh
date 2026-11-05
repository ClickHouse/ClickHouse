#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Comparisons of a Delta `timestamp` / `timestamp_ntz` column (ClickHouse `DateTime64(6)`) with a
# constant are handed to delta-kernel as timestamp literals, so it can skip files by their min/max
# statistics and partition values. Rows with the engine predicate on must match the rows with it off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_timestamp"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# As in 05153: everything is recorded first and executed in three batched clickhouse-local
# launches (fixtures, checks with the engine predicate off, checks with it on). Per-query scan
# counts are consecutive differences of the cumulative `DeltaLakeScannedFiles` counter.

TABLES=()
declare -A TABLE_DELTA_TYPE TABLE_STATS TABLE_PARTITIONED TABLE_NULLABLE TABLE_FILES
FIXTURE_QUERIES=""

# make_table <table> <timestamp|timestamp_ntz> <full|none|millis> <data|partition> <nullable> <file>...
# Each file is a comma-separated, sorted list of UTC timestamps, or NULL. A partition table takes one
# value per file as its partition value. `millis` writes the stats truncated to milliseconds, as Spark does.
make_table() {
    local table=$1 delta_type=$2 stats_mode=$3 layout=$4 nullable=$5
    shift 5
    mkdir -p "${ROOT}/${table}/_delta_log"
    TABLES+=("${table}")
    TABLE_DELTA_TYPE[$table]="${delta_type}"
    TABLE_STATS[$table]="${stats_mode}"
    TABLE_PARTITIONED[$table]="${layout}"
    TABLE_NULLABLE[$table]="${nullable}"
    TABLE_FILES[$table]="$(printf '%s;' "$@")"
    local i=0 values select
    for values in "$@"; do
        i=$((i + 1))
        if [[ "${layout}" == partition ]]; then
            select="SELECT toInt32(${i}) AS id"
        elif [[ "${values}" == NULL ]]; then
            select="SELECT CAST(NULL AS Nullable(DateTime64(6, 'UTC'))) AS ts"
        elif [[ "${nullable}" == true ]]; then
            select="SELECT toNullable(toDateTime64(arrayJoin(['${values//,/\',\'}']), 6, 'UTC')) AS ts"
        else
            select="SELECT toDateTime64(arrayJoin(['${values//,/\',\'}']), 6, 'UTC') AS ts"
        fi
        FIXTURE_QUERIES+="
            INSERT INTO FUNCTION file('${ROOT}/${table}/f${i}.parquet', Parquet) ${select};"
    done
}

# A JSON stats value: `timestamp` is an instant (`Z`), `timestamp_ntz` a wall-clock time.
stats_value() {
    local value=$1 delta_type=$2 stats_mode=$3
    if [[ "${stats_mode}" == millis && "${value}" == *.* ]]; then
        value="${value%.*}.$(printf '%s' "${value##*.}" | cut -c1-3)"
    fi
    value="${value/ /T}"
    [[ "${delta_type}" == timestamp ]] && value+="Z"
    printf '%s' "${value}"
}

# The hand-written `_delta_log`, written after the fixture batch because it embeds file sizes.
write_log() {
    local table=$1
    local delta_type="${TABLE_DELTA_TYPE[$table]}" stats_mode="${TABLE_STATS[$table]}"
    local layout="${TABLE_PARTITIONED[$table]}" nullable="${TABLE_NULLABLE[$table]}"
    local log="${ROOT}/${table}/_delta_log/00000000000000000000.json"
    local protocol='{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}'
    [[ "${delta_type}" == timestamp_ntz ]] && \
        protocol='{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}'
    local fields="{\\\"name\\\":\\\"ts\\\",\\\"type\\\":\\\"${delta_type}\\\",\\\"nullable\\\":${nullable},\\\"metadata\\\":{}}"
    local partition_columns=""
    if [[ "${layout}" == partition ]]; then
        fields="{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,\\\"metadata\\\":{}},${fields}"
        partition_columns='"ts"'
    fi
    cat > "${log}" <<EOF
${protocol}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-${table}","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[${fields}]}","partitionColumns":[${partition_columns}],"configuration":{}}}
EOF
    local i=0 file stats size partition_values
    local -a files values
    IFS=';' read -r -a files <<< "${TABLE_FILES[$table]}"
    for file in "${files[@]}"; do
        i=$((i + 1))
        IFS=, read -r -a values <<< "${file}"
        partition_values="{}"
        stats="{\"numRecords\":${#values[@]}"
        if [[ "${layout}" == partition ]]; then
            partition_values="{\"ts\":\"${file}\"}"
        elif [[ "${stats_mode}" != none ]]; then
            if [[ "${file}" == NULL ]]; then
                stats+=",\"nullCount\":{\"ts\":1}"
            else
                stats+=",\"minValues\":{\"ts\":\"$(stats_value "${values[0]}" "${delta_type}" "${stats_mode}")\"}"
                stats+=",\"maxValues\":{\"ts\":\"$(stats_value "${values[-1]}" "${delta_type}" "${stats_mode}")\"}"
                stats+=",\"nullCount\":{\"ts\":0}"
            fi
        fi
        stats+="}"
        size=$(wc -c < "${ROOT}/${table}/f${i}.parquet")
        cat >> "${log}" <<EOF
{"add":{"path":"f${i}.parquet","partitionValues":${partition_values},"size":${size},"modificationTime":1700000000000,"dataChange":true,"stats":"${stats//\"/\\\"}"}}
EOF
    done
}

CHECK_TABLES=() CHECK_WHERES=() CHECK_SETTINGS=()
OFF_ROWS=() OFF_DELTAS=() ON_ROWS=() ON_DELTAS=()

# check <table> <where> [settings]: rows with the engine predicate on must match the rows with
# it off, the off run must scan every file, and the on rows and scan count go to the reference.
check() {
    CHECK_TABLES+=("$1")
    CHECK_WHERES+=("$2")
    CHECK_SETTINGS+=("${3:-}")
}

# Rows are printed in UTC whatever the session time zone of the case.
data_query() {
    printf "SELECT arraySort(groupArray(ifNull(toString(ts, 'UTC'), 'NULL'))) FROM deltaLakeLocal('%s') WHERE %s%s;" \
        "${ROOT}/$1" "$2" "${3:+ SETTINGS $3}"
}

# Disable every applicable Parquet pruning path, so only the engine predicate can skip files.
run_batch() {
    local enabled=$1
    shift
    ${CLICKHOUSE_LOCAL} --session_timezone=UTC --convert_query_to_cnf=0 \
        --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0 \
        --input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 \
        --delta_lake_enable_engine_predicate="${enabled}" --delta_lake_throw_on_engine_predicate_error=1 "$@"
}

run_checks() {
    local enabled=$1
    local -n rows_out=$2 deltas_out=$3
    local i batch="" out
    for i in "${!CHECK_TABLES[@]}"; do
        batch+="$(data_query "${CHECK_TABLES[$i]}" "${CHECK_WHERES[$i]}" "${CHECK_SETTINGS[$i]}")
            SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"
    done
    out=$(run_batch "${enabled}" --query "${batch}")
    local -a lines
    mapfile -t lines <<< "${out}"
    if [[ "${#lines[@]}" != "$(( 2 * ${#CHECK_TABLES[@]} ))" ]]; then
        printf 'Expected %s lines from the check batch (engine predicate %s), got %s:\n%s\n' \
            "$(( 2 * ${#CHECK_TABLES[@]} ))" "${enabled}" "${#lines[@]}" "${out}" >&2
        return 1
    fi
    local previous=0 cumulative
    for i in "${!CHECK_TABLES[@]}"; do
        rows_out+=("${lines[2 * i]}")
        cumulative="${lines[2 * i + 1]}"
        deltas_out+=("$(( cumulative - previous ))")
        previous="${cumulative}"
    done
}

emit_results() {
    local idx
    for idx in "${!CHECK_TABLES[@]}"; do
        local table="${CHECK_TABLES[$idx]}" where="${CHECK_WHERES[$idx]}" settings="${CHECK_SETTINGS[$idx]}"
        local files=("${ROOT}/${table}/"*.parquet)
        printf '%s: %s%s\n' "${table}" "${where}" "${settings:+ SETTINGS ${settings}}"
        diff -u <(printf '%s\n' "${OFF_ROWS[$idx]}") <(printf '%s\n' "${ON_ROWS[$idx]}")
        if [[ "${OFF_DELTAS[$idx]}" != "${#files[@]}" ]]; then
            printf 'Unexpected unpruned file count %s for: %s\n' "${OFF_DELTAS[$idx]}" "${where}" >&2
            return 1
        fi
        printf '%s\n' "${ON_ROWS[$idx]}"
        printf '%s\n' "${ON_DELTAS[$idx]}"
    done
}

run_all() {
    ${CLICKHOUSE_LOCAL} --query "${FIXTURE_QUERIES}"
    local table
    for table in "${TABLES[@]}"; do
        write_log "${table}"
    done
    run_checks 0 OFF_ROWS OFF_DELTAS
    run_checks 1 ON_ROWS ON_DELTAS
    emit_results
}

# Data files are at least a millisecond apart: delta-kernel lowers a literal compared with a max
# statistic by 999 microseconds, because Spark truncates timestamp statistics to milliseconds.
# The last-but-one file lies beyond 2262-04-11, where a column scaled to nanoseconds overflows `Int64`.
DATA_FILES=(
    '1969-12-31 23:59:59.999999'
    '1970-01-01 00:00:00.000000'
    '2026-01-01 00:00:00.000000'
    '2026-01-01 00:00:00.123456'
    '2026-01-01 00:00:01.000000'
    '2262-04-12 00:00:00.000000'
    '2026-01-02 00:00:00.000000,2026-01-03 00:00:00.000000'
)
make_table ts timestamp full data false "${DATA_FILES[@]}"
make_table ntz timestamp_ntz full data false "${DATA_FILES[@]}"

NS_BETWEEN="toDateTime64('2026-01-01 00:00:00.123456500', 9, 'UTC')"
NS_EXACT="toDateTime64('2026-01-01 00:00:00.123456000', 9, 'UTC')"
NS_BEFORE_EPOCH="toDateTime64('1969-12-31 23:59:59.9999995', 9, 'UTC')"

for table in ts ntz; do
    # String constants are parsed as the column type, in its time zone.
    check "${table}" "ts = '2026-01-01 00:00:00.123456'"
    check "${table}" "'2026-01-01 00:00:00.123456' = ts"
    check "${table}" "ts < '2026-01-01 00:00:00.123456'"
    check "${table}" "ts >= '2026-01-01 00:00:01'"
    check "${table}" "ts = '2026-01-01 05:30:00.123456'" "session_timezone='Asia/Kolkata'"
    # Date and DateTime constants are cast to the column type: midnight in the column's time zone.
    check "${table}" "ts >= toDate('2026-01-02')"
    check "${table}" "ts < toDate32('1970-01-01')"
    check "${table}" "ts < toDate('2026-01-01')" "session_timezone='America/New_York'"
    check "${table}" "ts >= toDateTime('2026-01-01 00:00:01', 'UTC')"
    check "${table}" "ts < toDateTime('2026-01-01 05:30:01', 'Asia/Kolkata')"
done

# `DateTime64` constants compare by ticks, whatever their time zone; a coarser scale converts exactly.
check ts "ts = toDateTime64('2026-01-01 05:30:00.123456', 6, 'Asia/Kolkata')"
check ts "toDateTime64('2026-01-01 00:00:00.123456', 6, 'UTC') = ts"
check ts "ts >= toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')"
check ts "ts < toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')"

# A finer constant scales the column up. Between two microseconds the bound is the floor of the
# constant; truncating it instead would lose the file at 00:00:00.123456 for `<` and `<=`.
check ts "ts < ${NS_BETWEEN}"
check ts "ts <= ${NS_BETWEEN}"
check ts "ts > ${NS_BETWEEN}"
check ts "ts >= ${NS_BETWEEN}"
check ts "${NS_BETWEEN} > ts"
check ts "${NS_BETWEEN} >= ts"
check ts "${NS_BETWEEN} < ts"
check ts "${NS_BETWEEN} <= ts"
check ntz "ts < ${NS_BETWEEN}"
check ntz "${NS_BETWEEN} < ts"
# No column value equals such a constant: `=` and `!=` stay untranslated, a negation stays exact.
check ts "ts = ${NS_BETWEEN}"
check ts "ts != ${NS_BETWEEN}"
check ts "NOT (ts < ${NS_BETWEEN})"
# An exact constant is fenced: the file beyond 2262-04-11 stays scanned in either polarity.
check ts "ts = ${NS_EXACT}"
check ts "NOT (ts = ${NS_EXACT})"
check ts "ts < ${NS_BETWEEN}" "decimal_check_overflow=0"
check ts "ts <= ${NS_BEFORE_EPOCH}"

# Junctions and nested negation.
check ts "ts BETWEEN '2026-01-01 00:00:00' AND '2026-01-01 00:00:00.5'"
check ts "ts < '1970-01-01' OR ts > '2100-01-01'"
check ts "NOT (ts = '2026-01-01 00:00:00.123456' AND ts > '1970-01-01')"

# Untranslated forms scan every file.
check ts "ts IN ('2026-01-01 00:00:00.123456', '2026-01-02 00:00:00')"
check ts "toDate(ts) = '2026-01-01'"

make_table nullable timestamp full data true NULL '2026-01-01 00:00:00.000000' '2026-01-01 00:00:01.000000'
check nullable "ts = '2026-01-01 00:00:00'"
check nullable "ts IS NULL OR ts > '2026-01-01 00:00:00.5'"

make_table no_stats timestamp none data false '2026-01-01 00:00:00.000000' '2026-01-01 00:00:00.123456'
check no_stats "ts = '2026-01-01 00:00:00.123456'"

# Spark truncates the stats to milliseconds: the stored max 00:00:00.123 is below the row 00:00:00.123456.
make_table millis timestamp millis data false '2026-01-01 00:00:00.123456' '2026-01-01 00:00:01.000000'
check millis "ts > '2026-01-01 00:00:00.123400'"
check millis "ts = '2026-01-01 00:00:00.123456'"

# Partition values are exact: no millisecond allowance, and the bounds are tight.
PARTITION_FILES=(
    '1969-12-31 23:59:59.999999'
    '1970-01-01 00:00:00.000000'
    '2026-01-01 00:00:00.123455'
    '2026-01-01 00:00:00.123456'
    '2026-01-01 00:00:00.123457'
)
make_table part timestamp full partition false "${PARTITION_FILES[@]}"
make_table part_ntz timestamp_ntz full partition false "${PARTITION_FILES[@]}"
for table in part part_ntz; do
    check "${table}" "ts > '2026-01-01 00:00:00.123456'"
    check "${table}" "ts >= '2026-01-01 00:00:00.123456'"
    check "${table}" "ts = '2026-01-01 00:00:00.123456'"
    check "${table}" "ts != '2026-01-01 00:00:00.123456'"
    check "${table}" "ts = '2026-01-01 05:30:00.123456'" "session_timezone='Asia/Kolkata'"
    check "${table}" "ts < ${NS_BETWEEN}"
    check "${table}" "ts > ${NS_BEFORE_EPOCH}"
    check "${table}" "ts <= ${NS_BEFORE_EPOCH}"
done

run_all
