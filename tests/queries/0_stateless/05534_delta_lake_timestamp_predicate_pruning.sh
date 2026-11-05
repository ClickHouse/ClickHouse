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

# As in 05153: everything is recorded first and executed in batched clickhouse-local launches
# (fixtures, checks with the engine predicate off, on, and on under the old analyzer). Per-query scan counts are consecutive differences of the cumulative
# `DeltaLakeScannedFiles` counter.

TABLES=()
declare -A TABLE_DELTA_TYPE TABLE_STATS TABLE_PARTITIONED TABLE_NULLABLE TABLE_FILES TABLE_SOURCE TABLE_DIR
FIXTURE_QUERIES=""
SETUP_QUERIES=""

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

# copy_table <table> <timestamp|timestamp_ntz> <parquet> <values>: a one-file table over a prepared
# Parquet file holding <values>, with full statistics.
copy_table() {
    local table=$1 delta_type=$2 parquet=$3 values=$4
    mkdir -p "${ROOT}/${table}/_delta_log"
    cp "${parquet}" "${ROOT}/${table}/f1.parquet"
    TABLES+=("${table}")
    TABLE_DELTA_TYPE[$table]="${delta_type}"
    TABLE_STATS[$table]=full
    TABLE_PARTITIONED[$table]=data
    TABLE_NULLABLE[$table]=true
    TABLE_FILES[$table]="${values};"
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

# declare_table <name> <table> <columns> [settings]: a `DeltaLakeLocal` table over the files of <table>,
# with the columns declared by the user instead of read from the Delta schema (when <columns> is not
# empty) and with the table <settings>.
declare_table() {
    local name=$1 table=$2 columns=$3 settings=${4:-}
    TABLE_SOURCE[$name]="${name}"
    TABLE_DIR[$name]="${table}"
    SETUP_QUERIES+="
        CREATE TABLE IF NOT EXISTS ${name}${columns:+ (${columns})} ENGINE = DeltaLakeLocal('${ROOT}/${table}')${settings:+ SETTINGS ${settings}};"
}

ITEMS=()
CHECK_TABLES=() CHECK_WHERES=() CHECK_SETTINGS=() CHECK_PRINT=()
OFF_ROWS=() OFF_DELTAS=() ON_ROWS=() ON_DELTAS=() OLD_ROWS=() OLD_DELTAS=()

record_check() {
    ITEMS+=("check ${#CHECK_TABLES[@]}")
    CHECK_PRINT+=("$1")
    CHECK_TABLES+=("$2")
    CHECK_WHERES+=("$3")
    CHECK_SETTINGS+=("${4:-}")
}

# check <table> <where> [settings]: rows with the engine predicate on, with or without the analyzer,
# must match the rows with it off, the off run must scan every file, the old analyzer must scan as many
# files as the new one, and the on rows and scan count go to the reference.
check() { record_check all "$@"; }
# check_files: the same, but only the scan count goes to the reference.
check_files() { record_check files "$@"; }

# Rows are printed in UTC whatever the session time zone of the case.
data_query() {
    printf "SELECT arraySort(groupArray(ifNull(toString(ts, 'UTC'), 'NULL'))) FROM %s WHERE %s%s;" \
        "${TABLE_SOURCE[$1]:-deltaLakeLocal('${ROOT}/$1')}" "$2" "${3:+ SETTINGS $3}"
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
    shift 3
    local i batch="${SETUP_QUERIES}" out
    for i in "${!CHECK_TABLES[@]}"; do
        batch+="$(data_query "${CHECK_TABLES[$i]}" "${CHECK_WHERES[$i]}" "${CHECK_SETTINGS[$i]}")
            SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"
    done
    out=$(run_batch "${enabled}" "$@" --query "${batch}")
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
    local item kind idx
    for item in "${ITEMS[@]}"; do
        read -r kind idx <<< "${item}"
        local table="${CHECK_TABLES[$idx]}" where="${CHECK_WHERES[$idx]}" settings="${CHECK_SETTINGS[$idx]}"
        local files=("${ROOT}/${TABLE_DIR[$table]:-${table}}/"*.parquet)
        printf '%s: %s%s\n' "${table}" "${where}" "${settings:+ SETTINGS ${settings}}"
        diff -u <(printf '%s\n' "${OFF_ROWS[$idx]}") <(printf '%s\n' "${ON_ROWS[$idx]}")
        diff -u <(printf '%s\n' "${OFF_ROWS[$idx]}") <(printf '%s\n' "${OLD_ROWS[$idx]}")
        if [[ "${OFF_DELTAS[$idx]}" != "${#files[@]}" ]]; then
            printf 'Unexpected unpruned file count %s for: %s\n' "${OFF_DELTAS[$idx]}" "${where}" >&2
            return 1
        fi
        if [[ "${OLD_DELTAS[$idx]}" != "${ON_DELTAS[$idx]}" ]]; then
            printf 'Scanned %s files with the old analyzer and %s with the new one for: %s\n' \
                "${OLD_DELTAS[$idx]}" "${ON_DELTAS[$idx]}" "${where}" >&2
            return 1
        fi
        if [[ "${CHECK_PRINT[$idx]}" == all ]]; then
            printf '%s\n' "${ON_ROWS[$idx]}"
        fi
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
    run_checks 1 OLD_ROWS OLD_DELTAS --enable_analyzer=0
    emit_results
}

# Apart from the two files around the epoch, data files are at least a millisecond apart: delta-kernel
# lowers a literal compared with a max statistic by 999 microseconds, because Spark truncates timestamp
# statistics to milliseconds. The last-but-one file lies beyond 2262-04-11, outside the `Int64` nanoseconds:
# a row there may compare differently from its statistics (see below), so that file is not skipped when the
# comparison scales the column up or the constant is on or beyond that range.
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
    # A string finer than microseconds or outside the range is parsed by the comparison and by pruning alike.
    check "${table}" "ts < '2026-01-01 00:00:00.1234565'"
    check "${table}" "ts >= '2026-01-01 00:00:00.1234565'"
    check "${table}" "ts > '2300-01-01 00:00:00'"
    check "${table}" "ts < '1899-12-31 00:00:00'"
done

# A string is parsed in `cast_string_to_date_time_mode`, as the comparison does, whatever
# `date_time_input_format` says: '20260102' is 2026-01-02 in `best_effort` and 20260102 seconds after the
# epoch in `basic`.
check ts "ts < '20260102'" "date_time_input_format='basic'"
check ts "ts >= '20260102'" "cast_string_to_date_time_mode='basic'"

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
# With `decimal_check_overflow = 0` the comparison multiplies the column and wraps around beyond
# 2262-04-11, so the file beyond stays scanned in either polarity.
check ts "ts = ${NS_EXACT}"
check ts "NOT (ts = ${NS_EXACT})"
check ts "ts < ${NS_EXACT}"
check ts "${NS_EXACT} <= ts"
check ts "ts = ${NS_EXACT}" "decimal_check_overflow=0"
check ts "NOT (ts = ${NS_EXACT})" "decimal_check_overflow=0"
check ts "ts < ${NS_BETWEEN}" "decimal_check_overflow=0"
check ts "ts <= ${NS_BEFORE_EPOCH}"
check ntz "ts < ${NS_BETWEEN}" "decimal_check_overflow=0"
# With `date_time_overflow_behavior = 'saturate'` the Parquet reader clamps an INT96 timestamp to the
# `Int64` nanoseconds while its statistics keep the real value. Clamping keeps the order with a constant
# inside that range, so such a constant still skips the file beyond (here an INT64, read exactly).
check ts "ts = '2026-01-01 00:00:00.123456'" "date_time_overflow_behavior='saturate'"
check ntz "ts = '2026-01-01 00:00:00.123456'" "date_time_overflow_behavior='saturate'"

# The INT96 9999-12-31 23:59:59 is read as 2262-04-11 23:47:16.854775 under 'saturate', while the statistics
# keep 9999-12-31, so the file must not be skipped for the clamped value.
copy_table int96 timestamp "${CUR_DIR}/data_parquet/05534_int96_beyond_int64_nanoseconds.parquet" '9999-12-31 23:59:59.000000'
check int96 "ts = '2262-04-11 23:47:16.854775'" "date_time_overflow_behavior='saturate'"
check int96 "ts < '2262-04-12 00:00:00'" "date_time_overflow_behavior='saturate'"
check int96 "ts < '2262-04-11 23:47:16.854774'" "date_time_overflow_behavior='saturate'"
check int96 "ts > '2026-01-01 00:00:00'" "date_time_overflow_behavior='saturate'"
# The reader of a table engine takes the setting from the table, not from the query.
declare_table int96_saturate int96 "" "date_time_overflow_behavior='saturate'"
check int96_saturate "ts < '2262-04-12 00:00:00'"

# Junctions and nested negation.
check ts "ts BETWEEN '2026-01-01 00:00:00' AND '2026-01-01 00:00:00.5'"
check ts "NOT (ts = '2026-01-01 00:00:00.123456' AND ts > '1970-01-01')"
# An untranslated child under negation leaves the whole conjunction undecided.
check ts "NOT (ts = ${NS_BETWEEN} AND ts > '1970-01-01')"

# Untranslated forms scan every file.
check ts "ts < '1970-01-01' OR ts > '2100-01-01'"
check ts "ts IN ('2026-01-01 00:00:00.123456', '2026-01-02 00:00:00')"
check ts "toDate(ts) = '2026-01-01'"

# A column declared with another scale is read with a conversion that pruning does not mirror, so
# every file is scanned. Another time zone keeps the ticks and prunes as the Delta schema does.
# The files stay within the nanosecond `DateTime64(9)` range.
make_table small timestamp full data false '2026-01-01 00:00:00.000000' '2026-01-01 00:00:00.123456' '2026-01-01 00:00:01.000000'
declare_table ts_ms small "ts DateTime64(3, 'UTC')"
declare_table ts_ns small "ts DateTime64(9, 'UTC')"
declare_table ts_kolkata small "ts DateTime64(6, 'Asia/Kolkata')"
check_files ts_ms "ts = '2026-01-01 00:00:00.123'"
check_files ts_ms "ts >= '2026-01-01 00:00:01'"
check_files ts_ns "ts = '2026-01-01 00:00:00.123456'"
check_files ts_ns "ts >= '2026-01-01 00:00:01'"
check_files ts_ms "NOT (ts = '2026-01-01 00:00:00.123' AND ts > '1970-01-01')"
check ts_kolkata "ts = '2026-01-01 05:30:00.123456'"
check ts_kolkata "ts < '2026-01-01 05:30:00.123456'"
check ts_kolkata "ts < toDate('2026-01-01')"
check ts_kolkata "ts >= toDateTime('2026-01-01 00:00:01', 'UTC')"

make_table nullable timestamp full data true NULL '2026-01-01 00:00:00.000000' '2026-01-01 00:00:01.000000'
check nullable "ts = '2026-01-01 00:00:00'"
check nullable "'2026-01-01 00:00:00.5' < ts"
check nullable "ts < ${NS_BETWEEN}"
check nullable "ts < ${NS_BETWEEN}" "decimal_check_overflow=0"
check nullable "NOT (ts <= '2026-01-01 00:00:00.5')"
check nullable "ts IS NOT NULL AND ts >= '2026-01-01 00:00:01'"
check nullable "ts IS NULL OR ts > '2026-01-01 00:00:00.5'"
# A column declared not `Nullable` reads a NULL as 1970-01-01, which the statistics leave out: no file is skipped.
declare_table nullable_as_default nullable "ts DateTime64(6, 'UTC')"
check nullable_as_default "ts < '2000-01-01 00:00:00'"
check nullable_as_default "ts = toDateTime64(0, 6, 'UTC')"
check nullable_as_default "NOT (ts >= '2026-01-01 00:00:00')"

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
    check "${table}" "ts > '2026-01-01 00:00:00.1234565'"
    check "${table}" "ts <= '2026-01-01 00:00:00.1234565'"
    check "${table}" "ts > '2300-01-01 00:00:00'"
    check "${table}" "ts < '1899-12-31 00:00:00'"
done

run_all

# A literal visitor that throws leaves the comparison undecided, also inside a fenced comparison: the
# rows match the rows with the engine predicate off, and every file is scanned. The failpoint fires once,
# so each case runs in its own launch, after a run with the engine predicate off.
expect_failpoint_fallback() {
    local where=$1 settings=${2:+, $2}
    local -a lines
    mapfile -t lines < <(run_batch 1 --query "
        $(data_query ts "${where}" "delta_lake_enable_engine_predicate=0${settings}")
        SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;
        SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor;
        $(data_query ts "${where}" "delta_lake_throw_on_engine_predicate_error=0${settings}")
        SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;")
    printf 'failpoint: %s%s\n' "${where}" "${settings}"
    diff -u <(printf '%s\n' "${lines[0]}") <(printf '%s\n' "${lines[2]}")
    printf '%s\n' "${lines[2]}" "$(( lines[3] - lines[1] ))"
}
expect_failpoint_fallback "NOT (ts >= '2026-01-01 00:00:01')"
expect_failpoint_fallback "NOT (ts < ${NS_BETWEEN})" "decimal_check_overflow=0"

# A `timestamp_ntz` column widened from `date` (delta-kernel-rs `type-widening`): its old file holds a
# Parquet DATE, read as midnight in the session time zone, while delta-kernel reads the statistic
# "2024-09-09" as midnight wall-clock time. The comparison stays untranslated, so no file is skipped.
WIDENED="deltaLakeLocal('${CUR_DIR}/data_delta_lake/kernel_data/type-widening')"
mapfile -t lines < <(run_batch 1 --query "
    SELECT arraySort(groupArray(toString(date_timestamp_ntz, 'UTC'))) FROM ${WIDENED}
        WHERE date_timestamp_ntz >= '2024-09-09 00:00:00'
        SETTINGS session_timezone = 'America/New_York', delta_lake_enable_engine_predicate = 0;
    SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;
    SELECT arraySort(groupArray(toString(date_timestamp_ntz, 'UTC'))) FROM ${WIDENED}
        WHERE date_timestamp_ntz >= '2024-09-09 00:00:00'
        SETTINGS session_timezone = 'America/New_York';
    SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;")
printf 'widened: %s\n' "date_timestamp_ntz >= '2024-09-09 00:00:00'"
diff -u <(printf '%s\n' "${lines[0]}") <(printf '%s\n' "${lines[2]}")
printf '%s\n' "${lines[2]}" "$(( lines[3] - lines[1] ))"

run_batch 1 --query "
    SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor;
    $(data_query ts "NOT (ts >= '2026-01-01 00:00:01')")" 2>&1 | grep -c "FAULT_INJECTED"
