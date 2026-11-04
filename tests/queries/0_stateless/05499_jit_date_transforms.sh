#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: JIT compilation is not available in fasttest

# The date transforms that only read the lookup table of the day (`toYear`, `toMonth`, `toDayOfMonth`, `toQuarter`,
# `toDaysInMonth`, `toYYYYMM`, `toYYYYMMDD`) are compiled, reading the table of the time zone the interpreted function
# uses: the one of the argument, or the session's. Every transform of the family runs on edge values (daylight saving changes, the ends of the ranges) of `DateTime` in three time zones, of
# `Date` and of `Date32`, compiled and interpreted, in two session time zones one after the other, so that compiled code
# of one time zone is never used for another. The reference counts the combinations that ran compiled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FUNCTIONS=(toDayOfYear toISOYear toMillisecond toRelativeYearNum toYear toYearNumSinceEpoch toDaysSinceYearZero toMicrosecond
    toMonthNumSinceEpoch toNanosecond toRelativeDayNum toRelativeHourNum toRelativeMinuteNum toRelativeMonthNum
    toRelativeQuarterNum toRelativeSecondNum toRelativeWeekNum toYYYYMMDD toYYYYMM toYYYYMMDDhhmmss toDayOfMonth
    toDaysInMonth toHour toISOWeek toMinute toMonth toQuarter toSecond timezoneOffset toTimeWithFixedDate)
# The parts of the time of day throw for a date, so they only run on the `DateTime` columns.
TIME_PARTS=" toMillisecond toMicrosecond toNanosecond toHour toMinute toSecond timezoneOffset toTimeWithFixedDate "

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_date_transforms"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_date_transforms
    (k UInt32, t DateTime, t_ny DateTime('America/New_York'), t_kolkata DateTime('Asia/Kolkata'), d Date, d32 Date32)
    ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_date_transforms
    SELECT number, toDateTime(ts, 'UTC'), toDateTime(ts, 'UTC'), toDateTime(ts, 'UTC'), toDate(ts / 86400), toDate32(days)
    FROM
    (
        SELECT number,
            arrayElement([0, 1, 59, 3599, 86399, 951782400, 1615705199, 1615705200, 1636264799, 1636264800,
                1711846799, 1711846800, 1729990800, 1729994400, 2147483647, 4294967295], number % 16 + 1) AS ts,
            arrayElement([-25567, -1, 0, 1, 10956, 11016, 18321, 47482, 120529, -12345, 59, 60, 365, 366, 54786, 100000], intDiv(number, 16) % 16 + 1) AS days
        FROM numbers(256)
    )"

queries="$CLICKHOUSE_TMP/jit_date_transforms_queries.sql"
: > "$queries"
for f in "${FUNCTIONS[@]}"; do
    for column in t t_ny t_kolkata d d32; do
        if [[ ( "$column" == d || "$column" == d32 ) && "$TIME_PARTS" == *" $f "* ]]; then
            continue
        fi
        echo "SELECT '$f $column', groupArray(identity($f($column))) FROM jit_date_transforms;" >> "$queries"
    done
done

function run()
{
    # shellcheck disable=SC2068
    $CLICKHOUSE_CLIENT $@ --send_logs_level none --max_threads 1 --ignore-error --queries-file "$queries" 2>&1 \
        | sed -E 's/^.*Code: ([0-9]+)\..*DB::Exception: ([^:(]*[^:( ]).*/Code \1: \2/'
}

for timezone in Europe/Berlin Asia/Kathmandu; do
    compiled=$(run --session_timezone "$timezone" --compile_expressions 1 --min_count_to_compile_expression 0 \
        --log_comment "jit_date_transforms_${CLICKHOUSE_DATABASE}_$timezone")
    interpreted=$(run --session_timezone "$timezone" --compile_expressions 0)
    if [ "$compiled" == "$interpreted" ]; then
        echo "session time zone $timezone: compiled and interpreted outputs are the same"
    else
        echo "session time zone $timezone: compiled and interpreted outputs differ:"
        diff <(echo "$interpreted") <(echo "$compiled") | head -20
    fi
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(ProfileEvents['CompiledFunctionRows'] > 0) AS compiled, count() AS combinations
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment LIKE 'jit_date_transforms_${CLICKHOUSE_DATABASE}_%' AND query LIKE 'SELECT \'%'
    FORMAT TSV"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_date_transforms"
