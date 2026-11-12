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
# (fixtures, checks with the engine predicate off, on, and on under the old analyzer). Per-query scan
# counts are consecutive differences of the cumulative `DeltaLakeScannedFiles` counter.

TABLES=()
# Raw microseconds of the values that `toDateTime64` cannot write: it saturates before 1900.
declare -A RAW_MICROS=(['1600-01-01 00:00:00.000000']=-11676096000000000)
# TABLE_SPEC: "<timestamp|timestamp_ntz> <full|none|millis> <data|partition> <nullable>".
declare -A TABLE_SPEC TABLE_FILES TABLE_SOURCE TABLE_DIR
FIXTURE_QUERIES=""
SETUP_QUERIES=""

register_table() {
    local table=$1
    mkdir -p "${ROOT}/${table}/_delta_log"
    TABLES+=("${table}")
    TABLE_SPEC[$table]="$2 $3 $4 $5"
}

# make_table <table> <timestamp|timestamp_ntz> <full|none|millis> <data|partition> <nullable> <file>...
# Each file is a comma-separated, sorted list of UTC timestamps, or NULL. A partition table takes one
# value per file as its partition value. `millis` writes the stats truncated to milliseconds, as Spark does.
make_table() {
    local table=$1 layout=$4 nullable=$5
    register_table "$@"
    shift 5
    TABLE_FILES[$table]="$(printf '%s;' "$@")"
    local i=0 values select
    for values in "$@"; do
        i=$((i + 1))
        if [[ "${layout}" == partition ]]; then
            select="SELECT toInt32(${i}) AS id"
        elif [[ "${values}" == NULL ]]; then
            select="SELECT CAST(NULL AS Nullable(DateTime64(6, 'UTC'))) AS ts"
        elif [[ -n "${RAW_MICROS[$values]:-}" ]]; then
            select="SELECT fromUnixTimestamp64Micro(toInt64(${RAW_MICROS[$values]}), 'UTC') AS ts"
        else
            select="SELECT toDateTime64(arrayJoin(['${values//,/\',\'}']), 6, 'UTC') AS ts"
            [[ "${nullable}" == true ]] && select="SELECT toNullable(ts) AS ts FROM (${select})"
        fi
        FIXTURE_QUERIES+="
            INSERT INTO FUNCTION file('${ROOT}/${table}/f${i}.parquet', Parquet) ${select};"
    done
}

# copy_table <table> <timestamp|timestamp_ntz> <parquet> <values>: a one-file table over a prepared
# Parquet file holding <values>, with full statistics.
copy_table() {
    register_table "$1" "$2" full data true
    cp "$3" "${ROOT}/$1/f1.parquet"
    TABLE_FILES[$1]="$4;"
}

# A JSON stats value: `timestamp` is an instant (`Z`), `timestamp_ntz` a wall-clock time.
stats_value() {
    local value=$1 delta_type=$2 stats_mode=$3
    [[ "${stats_mode}" == millis ]] && value="${value:0:23}"
    value="${value/ /T}"
    [[ "${delta_type}" == timestamp ]] && value+="Z"
    printf '%s' "${value}"
}

# The hand-written `_delta_log`, written after the fixture batch because it embeds file sizes.
write_log() {
    local table=$1 delta_type stats_mode layout nullable
    read -r delta_type stats_mode layout nullable <<< "${TABLE_SPEC[$table]}"
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
    local i=0 file stats partition_values
    local -a files values
    IFS=';' read -r -a files <<< "${TABLE_FILES[$table]}"
    for file in "${files[@]}"; do
        i=$((i + 1))
        IFS=, read -r -a values <<< "${file}"
        partition_values="{}"
        stats="{\"numRecords\":${#values[@]}"
        if [[ "${layout}" == partition ]]; then
            partition_values="{\"ts\":\"${file}\"}"
        elif [[ "${file}" == NULL ]]; then
            stats+=",\"nullCount\":{\"ts\":1}"
        elif [[ "${stats_mode}" != none ]]; then
            stats+=",\"minValues\":{\"ts\":\"$(stats_value "${values[0]}" "${delta_type}" "${stats_mode}")\"}"
            stats+=",\"maxValues\":{\"ts\":\"$(stats_value "${values[-1]}" "${delta_type}" "${stats_mode}")\"}"
            stats+=",\"nullCount\":{\"ts\":0}"
        fi
        stats+="}"
        cat >> "${log}" <<EOF
{"add":{"path":"f${i}.parquet","partitionValues":${partition_values},"size":$(wc -c < "${ROOT}/${table}/f${i}.parquet"),"modificationTime":1700000000000,"dataChange":true,"stats":"${stats//\"/\\\"}"}}
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

CHECK_TABLES=() CHECK_WHERES=() CHECK_SETTINGS=()
OFF_ROWS=() OFF_DELTAS=() ON_ROWS=() ON_DELTAS=() OLD_ROWS=() OLD_DELTAS=()

# check <table> <where> [settings]: rows with the engine predicate on, with or without the analyzer,
# must match the rows with it off, the off run must scan every file, the old analyzer must scan as many
# files as the new one, and the scan count goes to the reference.
check() {
    CHECK_TABLES+=("$1")
    CHECK_WHERES+=("$2")
    CHECK_SETTINGS+=("${3:-}")
}

# Rows are printed in UTC whatever the session time zone of the case.
data_query() {
    printf "SELECT arraySort(groupArray(ifNull(toString(ts, 'UTC'), 'NULL'))) FROM %s WHERE %s%s;" \
        "${TABLE_SOURCE[$1]:-deltaLakeLocal('${ROOT}/$1')}" "$2" "${3:+ SETTINGS $3}"
}

SCANNED="SELECT sumIf(value, event = 'DeltaLakeScannedFiles') FROM system.events;"

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
    local i batch="${SETUP_QUERIES}" out previous=0
    for i in "${!CHECK_TABLES[@]}"; do
        batch+="$(data_query "${CHECK_TABLES[$i]}" "${CHECK_WHERES[$i]}" "${CHECK_SETTINGS[$i]}") ${SCANNED}"
    done
    out=$(run_batch "${enabled}" "$@" --query "${batch}")
    local -a lines
    mapfile -t lines <<< "${out}"
    if [[ "${#lines[@]}" != "$(( 2 * ${#CHECK_TABLES[@]} ))" ]]; then
        printf 'Unexpected output of the check batch (engine predicate %s):\n%s\n' "${enabled}" "${out}" >&2
        return 1
    fi
    for i in "${!CHECK_TABLES[@]}"; do
        rows_out+=("${lines[2 * i]}")
        deltas_out+=("$(( lines[2 * i + 1] - previous ))")
        previous="${lines[2 * i + 1]}"
    done
}

run_all() {
    ${CLICKHOUSE_LOCAL} --query "${FIXTURE_QUERIES}"
    local table i
    for table in "${TABLES[@]}"; do
        write_log "${table}"
    done
    run_checks 0 OFF_ROWS OFF_DELTAS
    run_checks 1 ON_ROWS ON_DELTAS
    run_checks 1 OLD_ROWS OLD_DELTAS --enable_analyzer=0
    for i in "${!CHECK_TABLES[@]}"; do
        local where="${CHECK_WHERES[$i]}${CHECK_SETTINGS[$i]:+ SETTINGS ${CHECK_SETTINGS[$i]}}"
        local files=("${ROOT}/${TABLE_DIR[${CHECK_TABLES[$i]}]:-${CHECK_TABLES[$i]}}/"*.parquet)
        diff -u <(printf '%s\n' "${OFF_ROWS[$i]}") <(printf '%s\n' "${ON_ROWS[$i]}")
        diff -u <(printf '%s\n' "${OFF_ROWS[$i]}") <(printf '%s\n' "${OLD_ROWS[$i]}")
        if [[ "${OFF_DELTAS[$i]}" != "${#files[@]}" || "${OLD_DELTAS[$i]}" != "${ON_DELTAS[$i]}" ]]; then
            printf 'Scanned %s files with the predicate off (of %s), %s on, %s with the old analyzer for: %s\n' \
                "${OFF_DELTAS[$i]}" "${#files[@]}" "${ON_DELTAS[$i]}" "${OLD_DELTAS[$i]}" "${where}" >&2
            return 1
        fi
        printf '%s: %s\t%s\n' "${CHECK_TABLES[$i]}" "${where}" "${ON_DELTAS[$i]}"
    done
}

# Apart from the two files around the epoch, data files are at least a millisecond apart: delta-kernel
# lowers a literal compared with a max statistic by 999 microseconds, because Spark truncates timestamp
# statistics to milliseconds. The last-but-one file lies beyond 2262-04-11, outside the `Int64` nanoseconds:
# a comparison that scales the column up may compare a row there differently from its statistics (see below),
# so that file is not skipped for such a comparison.
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

# String constants are parsed as the column type, in its time zone.
check ts "ts = '2026-01-01 00:00:00.123456'"
check ts "'2026-01-01 00:00:00.123456' = ts"
check ts "ts < '2026-01-01 00:00:00.123456'"
check ts "ts = '2026-01-01 05:30:00.123456'" "session_timezone='Asia/Kolkata'"
# Date and DateTime constants are cast to the column type: midnight in the column's time zone.
check ts "ts >= toDate('2026-01-02')"
check ts "ts < toDate32('1970-01-01')"
check ts "ts < toDate('2026-01-01')" "session_timezone='America/New_York'"
check ts "ts < toDateTime('2026-01-01 05:30:01', 'Asia/Kolkata')"
# A string finer than microseconds or outside the range is parsed by the comparison and by pruning alike.
check ts "ts < '2026-01-01 00:00:00.1234565'"
check ts "ts > '2300-01-01 00:00:00'"
check ts "ts < '1899-12-31 00:00:00'"
# A `timestamp_ntz` column differs only in the kernel literal: a wall-clock time instead of an instant.
check ntz "ts = '2026-01-01 05:30:00.123456'" "session_timezone='Asia/Kolkata'"
check ntz "ts < toDate('2026-01-01')" "session_timezone='America/New_York'"

# A string is parsed in `cast_string_to_date_time_mode`, as the comparison does, whatever
# `date_time_input_format` says: '20260102' is 2026-01-02 in `best_effort` and 20260102 seconds after the
# epoch in `basic`.
check ts "ts < '20260102'" "date_time_input_format='basic'"
check ts "ts >= '20260102'" "cast_string_to_date_time_mode='basic'"
# The mode is the one the comparison was built with: here the outer query's, not the subquery's that reads.
# Pruning confirms the converted literal with the comparison itself, which carries that mode.
for mode in best_effort basic; do
    TABLE_SOURCE[ts_${mode}]="(SELECT * FROM deltaLakeLocal('${ROOT}/ts') SETTINGS cast_string_to_date_time_mode='${mode}')"
    TABLE_DIR[ts_${mode}]=ts
done
check ts_best_effort "ts > '20260102'" "cast_string_to_date_time_mode='basic'"
check ts_basic "ts < '20260102'"

# `DateTime64` constants compare by ticks, whatever their time zone; a coarser scale converts exactly.
check ts "ts = toDateTime64('2026-01-01 05:30:00.123456', 6, 'Asia/Kolkata')"
check ts "ts < toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')"

# A finer constant scales the column up. Between two microseconds the bound is the floor of the
# constant; truncating it instead would lose the file at 00:00:00.123456 for `<` and `<=`.
check ts "ts < ${NS_BETWEEN}"
check ts "ts >= ${NS_BETWEEN}"
check ts "${NS_BETWEEN} < ts"
check ts "${NS_BETWEEN} >= ts"
check ntz "ts < ${NS_BETWEEN}"
# No column value equals such a constant: `=` stays untranslated, a negation stays exact.
check ts "ts = ${NS_BETWEEN}"
check ts "NOT (ts < ${NS_BETWEEN})"
# With `decimal_check_overflow = 0` the comparison multiplies the column and wraps around beyond 2262-04-11.
# Pruning cannot read that setting of the comparison, so the file there stays scanned in either polarity,
# with the setting on too.
check ts "ts = ${NS_EXACT}"
check ts "ts = ${NS_EXACT}" "decimal_check_overflow=0"
check ts "NOT (ts = ${NS_EXACT})" "decimal_check_overflow=0"
check ts "ts < ${NS_BETWEEN}" "decimal_check_overflow=0"
check ts "ts <= ${NS_BEFORE_EPOCH}"
# With `date_time_overflow_behavior = 'saturate'` the Parquet reader clamps a timestamp to the range of
# `DateTime64(6)` while its statistics keep the real value. Clamping keeps the order with a constant inside
# that range, so such a constant still skips files as usual.
check ts "ts = '2026-01-01 00:00:00.123456'" "date_time_overflow_behavior='saturate'"

# The INT96 9999-12-31 23:59:59 lies beyond the `Int64` nanoseconds and is read as written, in any
# overflow mode, as its statistics say.
copy_table int96 timestamp "${CUR_DIR}/data_parquet/05534_int96_beyond_int64_nanoseconds.parquet" '9999-12-31 23:59:59.000000'
check int96 "ts = '9999-12-31 23:59:59'"
check int96 "ts < '2262-04-12 00:00:00'"
check int96 "ts < '2262-04-12 00:00:00'" "date_time_overflow_behavior='saturate'"
check int96 "ts >= '9999-12-31 23:59:59.999999'" "date_time_overflow_behavior='saturate'"
# The reader of a table engine takes the setting from the table, not from the query.
declare_table int96_saturate int96 "" "date_time_overflow_behavior='saturate'"
check int96_saturate "ts < '2262-04-12 00:00:00'"

# The INT96 1969-12-31 23:59:59.9999995 is read rounded towards zero as 1970-01-01 00:00:00, while Spark
# floors it to 23:59:59.999999 and truncates the statistics to 23:59:59.999: one microsecond beyond the
# millisecond that delta-kernel allows, so a constant at or before the epoch keeps such rows undecided.
copy_table int96_before_epoch timestamp "${CUR_DIR}/data_parquet/05534_int96_before_epoch_sub_microsecond.parquet" '1969-12-31 23:59:59.999'
check int96_before_epoch "ts = '1970-01-01 00:00:00'"
check int96_before_epoch "ts >= '1970-01-01 00:00:00'"
check int96_before_epoch "ts > '1970-01-01 00:00:00'"

# Junctions; an untranslated child under negation leaves the whole conjunction undecided.
check ts "ts BETWEEN '2026-01-01 00:00:00' AND '2026-01-01 00:00:00.5'"
check ts "NOT (ts = ${NS_BETWEEN} AND ts > '1970-01-01')"

# Untranslated forms scan every file.
check ts "ts IN ('2026-01-01 00:00:00.123456', '2026-01-02 00:00:00')"
check ts "toDate(ts) = '2026-01-01'"

# A column declared with another scale is read with a conversion that pruning does not mirror, so
# every file is scanned. Another time zone keeps the ticks and prunes as the Delta schema does.
# The files stay within the nanosecond `DateTime64(9)` range.
make_table small timestamp full data false '2026-01-01 00:00:00.000000' '2026-01-01 00:00:00.123456' '2026-01-01 00:00:01.000000'
declare_table ts_ms small "ts DateTime64(3, 'UTC')"
declare_table ts_ns small "ts DateTime64(9, 'UTC')"
declare_table ts_kolkata small "ts DateTime64(6, 'Asia/Kolkata')"
check ts_ms "ts = '2026-01-01 00:00:00.123'"
check ts_ns "ts = '2026-01-01 00:00:00.123456'"
check ts_kolkata "ts = '2026-01-01 05:30:00.123456'"
check ts_kolkata "ts < toDate('2026-01-01')"

make_table nullable timestamp full data true NULL '2026-01-01 00:00:00.000000' '2026-01-01 00:00:01.000000'
check nullable "ts = '2026-01-01 00:00:00'"
check nullable "NOT (ts <= '2026-01-01 00:00:00.5')"
check nullable "ts IS NULL OR ts > '2026-01-01 00:00:00.5'"
# A column declared not `Nullable` reads a NULL as 1970-01-01, which the statistics leave out: no file is skipped.
declare_table nullable_as_default nullable "ts DateTime64(6, 'UTC')"
check nullable_as_default "ts < '2000-01-01 00:00:00'"
check nullable_as_default "NOT (ts >= '2026-01-01 00:00:00')"

# Before 1677-09-21 a row also leaves the `Int64` nanoseconds: with `decimal_check_overflow = 0` the 1600 row
# wraps around and compares above the constant, so its file stays scanned whenever the column is scaled.
make_table ancient timestamp full data false '1600-01-01 00:00:00.000000' '2026-01-01 00:00:00.000000'
check ancient "ts > ${NS_BETWEEN}" "decimal_check_overflow=0"
check ancient "ts > ${NS_BETWEEN}"

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
check part "ts > '2026-01-01 00:00:00.123456'"
check part "ts = '2026-01-01 00:00:00.123456'"
check part "ts < ${NS_BETWEEN}"
check part "ts <= ${NS_BEFORE_EPOCH}"
check part "ts > '2300-01-01 00:00:00'"
check part_ntz "ts = '2026-01-01 05:30:00.123456'" "session_timezone='Asia/Kolkata'"
check part_ntz "ts > '2026-01-01 00:00:00.123456'"

run_all

# compare <label> <off query> <on query>: in one launch, the rows of <on query> must match the rows of
# <off query>; the rows and the files <on query> scans go to the reference.
compare() {
    local -a lines
    mapfile -t lines < <(run_batch 1 --query "$2 ${SCANNED} $3 ${SCANNED}")
    diff -u <(printf '%s\n' "${lines[0]}") <(printf '%s\n' "${lines[2]}")
    printf '%s\t%s\t%s\n' "$1" "${lines[2]}" "$(( lines[3] - lines[1] ))"
}

# A literal visitor that throws leaves the comparison undecided, also inside a fenced comparison: the
# rows match the rows with the engine predicate off, and every file is scanned. The failpoint fires once,
# so each case runs in its own launch.
for case in "NOT (ts >= '2026-01-01 00:00:01')|" "NOT (ts < ${NS_BETWEEN})|, decimal_check_overflow=0"; do
    where=${case%|*} settings=${case#*|}
    compare "failpoint: ${where}${settings}" \
        "$(data_query ts "${where}" "delta_lake_enable_engine_predicate=0${settings}")" \
        "SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor; $(data_query ts "${where}" "delta_lake_throw_on_engine_predicate_error=0${settings}")"
done

# A `timestamp_ntz` column widened from `date` (delta-kernel-rs `type-widening`): its old file holds a
# Parquet DATE, read as midnight in the session time zone, while delta-kernel reads the statistic
# "2024-09-09" as midnight wall-clock time. The comparison stays untranslated, so no file is skipped.
WIDENED="SELECT arraySort(groupArray(toString(date_timestamp_ntz, 'UTC')))
    FROM deltaLakeLocal('${CUR_DIR}/data_delta_lake/kernel_data/type-widening')
    WHERE date_timestamp_ntz >= '2024-09-09 00:00:00' SETTINGS session_timezone = 'America/New_York'"
compare "widened: date_timestamp_ntz >= '2024-09-09 00:00:00'" \
    "${WIDENED}, delta_lake_enable_engine_predicate = 0;" "${WIDENED};"

run_batch 1 --query "
    SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor;
    $(data_query ts "NOT (ts >= '2026-01-01 00:00:01')")" 2>&1 | grep -c "FAULT_INJECTED"
