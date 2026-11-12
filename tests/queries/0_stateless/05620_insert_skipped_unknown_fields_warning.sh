#!/usr/bin/env bash

# An INSERT whose data has a field with no matching column (`input_format_skip_unknown_fields`) logs a warning
# naming the field and the query: once per field per INSERT, at most 10 fields. Reads do not warn.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (a UInt64, b String DEFAULT 'b_default') ENGINE = Memory;
    CREATE TABLE t_agg (k UInt64, s AggregateFunction(sum, UInt64)) ENGINE = Memory;
    CREATE TABLE t_null (a UInt64) ENGINE = Null;"

# $1: table, $2: format, $3: more URL parameters. The data is read from stdin.
function insert()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&async_insert=0&$3&query=INSERT+INTO+$1+FORMAT+$2" --data-binary @-
}

for fmt in Native JSONEachRow TSKV BSONEachRow JSONColumns CSVWithNames JSONCompact; do
    $CLICKHOUSE_CLIENT -q "SELECT 1::UInt64 AS a, 'x' AS u_${fmt}_${DB}, 'y' AS u2_${fmt}_${DB} FORMAT $fmt" | insert t "$fmt"
done
printf 'a=2&u_form_%s=x' "$DB" | insert t Form
printf "INSERT INTO \`t\` (\`a\`, \`u_mysqldump_%s\`) VALUES (3, 'x');\n" "$DB" | insert t MySQLDump "input_format_mysql_dump_table_name=t"
printf '{"a":4,"u_optout_%s":1}' "$DB" | insert t JSONEachRow "input_format_warn_on_skipped_unknown_fields=0"
printf '{"a":5,"b":"y","u_after_%s":1}' "$DB" | insert t JSONEachRow
printf '{"a":6,"b":"y","a":7}' | insert t JSONEachRow "input_format_json_ignore_unnecessary_fields=1&query_id=${DB}_dup"
$CLICKHOUSE_CLIENT -q "SELECT number AS a, 1 AS u_parallel_${DB} FROM numbers(100000) FORMAT JSONEachRow" \
    | insert t_null JSONEachRow "input_format_parallel_parsing=1&max_parsing_threads=4&min_chunk_bytes_for_parallel_parsing=1024"
$CLICKHOUSE_CLIENT -q "SELECT number AS a, 1 AS u_serial_${DB} FROM numbers(100000) FORMAT JSONEachRow" \
    | insert t_null JSONEachRow "input_format_parallel_parsing=0"
printf '{"k":1,"s":5,"u_aggstate_%s":1}' "$DB" | insert t_agg JSONEachRow "aggregate_function_input_format=value"
printf '{"a":15,"u_input_%s":1}' "$DB" | insert "t+SELECT+*+FROM+input()" JSONEachRow

ASYNC="async_insert=1&wait_for_async_insert=1"
printf '{"a":8,"u_async_%s":1}' "$DB" | insert t JSONEachRow "$ASYNC&query_id=${DB}_async"
printf '{"a":9,"u_async_optout_%s":1}' "$DB" | insert t JSONEachRow "$ASYNC&input_format_warn_on_skipped_unknown_fields=0"
printf '{"a":10,"u_async_fmt_%s":1}' "$DB" | insert t TSV "$ASYNC&input_format=JSONEachRow"
# Two inserts of the same query and field, queued and then flushed together: each one gets its own warning.
for i in 1 2; do
    printf '{"a":%d,"u_batch_%s":1}' "$((10 + i))" "$DB" \
        | insert t JSONEachRow "async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_max_ms=10000000&query_id=${DB}_batch$i"
done
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE t"

$CLICKHOUSE_CLIENT -q "SELECT '{\"a\":13,' || arrayStringConcat(arrayMap(i -> concat('\"u_cap', leftPad(toString(i), 2, '0'), '_${DB}\":1'), range(1, 13)), ',') || '}' FORMAT TSVRaw" \
    | insert t JSONEachRow "query_id=${DB}_cap"

$CLICKHOUSE_CLIENT -q "SELECT a FROM format(JSONEachRow, 'a UInt64', '{\"a\":1,\"u_read_${DB}\":2}') FORMAT Null"
$CLICKHOUSE_CLIENT -q "INSERT INTO t (a) SELECT a FROM format(JSONEachRow, 'a UInt64', '{\"a\":14,\"u_insert_select_${DB}\":2}')"

# $1: a condition on `message` and `query_id`. Prints the number of warnings matching it.
function warnings()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 HOUR AND level = 'Warning' AND $1"
}

for _ in {1..30}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    [ "$(warnings "query_id = '${DB}_cap' AND position(message, 'more than 10 fields') > 0")" = 1 ] && break
    sleep 1
done

for f in u_Native u2_Native u_JSONEachRow u2_JSONEachRow u_TSKV u2_TSKV u_BSONEachRow u2_BSONEachRow u_JSONColumns u2_JSONColumns \
    u_CSVWithNames u2_CSVWithNames u_JSONCompact u2_JSONCompact u_form u_mysqldump u_optout u_after u_parallel u_serial u_aggstate u_input \
    u_async u_async_optout u_async_fmt u_read u_insert_select; do
    echo "$f $(warnings "position(message, 'Field \`${f}_${DB}\` ') > 0")"
done
echo "dup $(warnings "query_id = '${DB}_dup' AND position(message, 'has no matching column') > 0")"
echo "cap fields $(warnings "query_id = '${DB}_cap' AND position(message, 'has no matching column') > 0")"
echo "cap summary $(warnings "query_id = '${DB}_cap' AND position(message, 'more than 10 fields') > 0")"
echo "async query id $(warnings "position(message, 'Field \`u_async_${DB}\` of the input data of query ${DB}_async in format JSONEachRow ') > 0")"
echo "async format $(warnings "position(message, 'Field \`u_async_fmt_${DB}\` ') > 0 AND position(message, ' in format JSONEachRow ') > 0")"
for i in 1 2; do
    echo "batch $i $(warnings "position(message, 'Field \`u_batch_${DB}\` of the input data of query ${DB}_batch$i ') > 0")"
done

$CLICKHOUSE_CLIENT -q "SELECT * FROM t ORDER BY a, b; SELECT k, sumMerge(s) FROM t_agg GROUP BY k"
