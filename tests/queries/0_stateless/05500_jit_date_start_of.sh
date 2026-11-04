#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: JIT compilation is not available in fasttest

# `toStartOfDay`, `toStartOfHour` and the minute intervals are compiled from the start of the day in the lookup table of
# the time zone of their `DateTime` result. They run on edge values (the epoch, daylight saving changes, the end of the
# range) in time zones whose offset is a whole number of hours, of half hours and of quarter hours, and with half-hour
# daylight saving (Lord Howe), compiled and interpreted, in two session time zones one after the other.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FUNCTIONS=(toStartOfDay toStartOfHour toStartOfMinute toStartOfFiveMinutes toStartOfTenMinutes toStartOfFifteenMinutes)
COLUMNS=(t t_ny t_kolkata t_kathmandu t_lord_howe t_st_johns d)

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_date_start_of"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_date_start_of
    (k UInt32, t DateTime, t_ny DateTime('America/New_York'), t_kolkata DateTime('Asia/Kolkata'),
    t_kathmandu DateTime('Asia/Kathmandu'), t_lord_howe DateTime('Australia/Lord_Howe'), t_st_johns DateTime('America/St_Johns'), d Date)
    ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_date_start_of
    SELECT number, ts, ts, ts, ts, ts, ts, toDate(intDiv(ts, 86400) + number % 3)
    FROM
    (
        SELECT number,
            arrayElement([0, 1, 59, 3599, 3600, 19799, 19800, 86399, 951782400, 1615705199, 1615705200, 1636264799, 1636264800,
                1617465599, 1617465600, 1633190399, 1633190400, 1711846799, 2147483647, 4294967295, 4294900000, 1234567890], number % 22 + 1) AS ts
        FROM numbers(66)
    )"

queries="$CLICKHOUSE_TMP/jit_date_start_of_queries.sql"
: > "$queries"
for f in "${FUNCTIONS[@]}"; do
    for column in "${COLUMNS[@]}"; do
        if [[ "$column" == d && "$f" != toStartOfDay ]]; then
            continue
        fi
        echo "SELECT '$f $column', groupArray(identity($f($column))) FROM jit_date_start_of;" >> "$queries"
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
        --log_comment "jit_date_start_of_${CLICKHOUSE_DATABASE}_$timezone")
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
        AND log_comment LIKE 'jit_date_start_of_${CLICKHOUSE_DATABASE}_%' AND query LIKE 'SELECT \'%'
    FORMAT TSV"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_date_start_of"
