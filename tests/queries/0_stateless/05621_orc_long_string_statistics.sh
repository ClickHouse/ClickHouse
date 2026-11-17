#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no ORC format

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="$CUR_DIR/data_orc"
WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_orc_long_string_statistics"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "-- the writer keeps at most 1024 bytes of a string value in the statistics"
${CLICKHOUSE_LOCAL} -q "SELECT repeat('x', 1000000) AS s INTO OUTFILE '$WORK_DIR/one.orc' TRUNCATE FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
size=$(stat -c %s "$WORK_DIR/one.orc")
if [ "$size" -lt 1100000 ]; then echo "OK"; else echo "file size $size"; fi

echo "-- the upper bound stays valid UTF-8 when the last character is the largest code point"
${CLICKHOUSE_LOCAL} -q "SELECT concat(repeat('b', 1020), unhex('F48FBFBF'), 'tail') AS s INTO OUTFILE '$WORK_DIR/max_code_point.orc' TRUNCATE FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
${CLICKHOUSE_LOCAL} -q "
    SELECT countSubstrings(raw, concat(repeat('b', 1020), unhex('F48FBFC0'))) AS invalid_bounds,
        countSubstrings(raw, concat(repeat('b', 1019), 'c')) AS carried_bounds
    FROM file('$WORK_DIR/max_code_point.orc', RawBLOB, 'raw String')"

echo "-- the upper bound does not increment U+FFFF, which UTF-16 readers order after U+10000"
${CLICKHOUSE_LOCAL} -q "SELECT concat(repeat('b', 1021), unhex('EFBFBF'), 'tail') AS s INTO OUTFILE '$WORK_DIR/bmp_max.orc' TRUNCATE FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
${CLICKHOUSE_LOCAL} -q "
    SELECT countSubstrings(raw, concat(repeat('b', 1021), unhex('F0908080'))) AS supplementary_bounds,
        countSubstrings(raw, concat(repeat('b', 1020), 'c')) AS carried_bounds
    FROM file('$WORK_DIR/bmp_max.orc', RawBLOB, 'raw String')"

echo "-- without an upper bound, the exact maximum is kept, as readers need a maximum"
${CLICKHOUSE_LOCAL} -q "SELECT repeat(unhex('EFBFBF'), 400) AS s INTO OUTFILE '$WORK_DIR/no_upper_bound.orc' TRUNCATE FORMAT ORC SETTINGS output_format_orc_compression_method = 'none'"
# The value itself and the maximums of the row index, the stripe and the file.
${CLICKHOUSE_LOCAL} -q "SELECT countSubstrings(raw, repeat(unhex('EFBFBF'), 400)) AS values FROM file('$WORK_DIR/no_upper_bound.orc', RawBLOB, 'raw String')"
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('$WORK_DIR/no_upper_bound.orc') WHERE s = repeat(unhex('EFBFBF'), 400) SETTINGS input_format_orc_filter_push_down = 1"

# Each value is a row group of its own, so the pushdown can skip each of them.
${CLICKHOUSE_LOCAL} -q "
    SELECT n, s FROM values('n UInt32, s String',
        (0, repeat('a', 2000)),
        (1, repeat('b', 1024)),
        (2, repeat('b', 2000)),
        (3, concat(repeat('b', 1023), 'é', 'tail')),
        (4, concat(repeat('é', 1500), 'z')),
        (5, concat(repeat('b', 1020), unhex('F48FBFBF'), 'tail')),
        (6, concat(repeat('b', 1000), unhex(repeat('FF', 100)))),
        (7, 'c'))
    INTO OUTFILE '$WORK_DIR/groups.orc' TRUNCATE FORMAT ORC
    SETTINGS output_format_orc_row_index_stride = 1, output_format_orc_compression_method = 'none'"

# Prints the matching rows (the column `n` or the expression given) and how many rows the reader returned before the filter.
query()
{
    local file=$1 condition=$2 n=${3:-n}
    local result
    result=$(${CLICKHOUSE_LOCAL} --print-profile-events -q "
        SELECT groupArray(n) FROM (SELECT $n AS n FROM file('$file', ORC) WHERE $condition ORDER BY n)
        SETTINGS input_format_orc_filter_push_down = 1, input_format_orc_use_fast_decoder = 1" 2>"$WORK_DIR/events")
    local expected
    expected=$(${CLICKHOUSE_LOCAL} -q "
        SELECT groupArray(n) FROM (SELECT $n AS n FROM file('$file', ORC) WHERE $condition ORDER BY n)
        SETTINGS input_format_orc_filter_push_down = 0")
    local rows
    # The profile events can be reported in several parts, by different threads.
    rows=$(grep -o 'SelectedRows: [0-9]*' "$WORK_DIR/events" | awk '{ sum += $2 } END { print sum + 0 }')
    if [ "$result" != "$expected" ]; then echo "$condition: wrong result $result, expected $expected"; fi
    echo "$condition: $result, read $rows rows"
}

echo "-- the pushdown uses the bounds written instead of long minimums and maximums"
for condition in \
    "s = repeat('b', 2000)" \
    "s = repeat('b', 1024)" \
    "s < repeat('b', 1024)" \
    "s > repeat('b', 2000)" \
    "s IN (repeat('a', 2000), 'c')" \
    "s = concat(repeat('é', 1500), 'z')" \
    "s = concat(repeat('b', 1020), unhex('F48FBFBF'), 'tail')" \
    "s = concat(repeat('b', 1000), unhex(repeat('FF', 100)))" \
    "s = 'b'"
do
    query "$WORK_DIR/groups.orc" "$condition"
done

echo "-- the reader keeps only the bounds of long statistics in existing files"
# The file was written before the statistics were truncated: each statistic holds the whole 20 MB value.
${CLICKHOUSE_LOCAL} -q "SELECT length(s) FROM file('$DATA_DIR/long_string_statistics_20m.orc') SETTINGS max_memory_usage = '90M'"
for condition in "s = repeat('b', 2000)" "s > repeat('a', 2000)" "s < repeat('b', 2000)"
do
    query "$DATA_DIR/long_string_statistics_groups.orc" "$condition"
done

echo "-- a repeated minimum replaces the previous one, also when it is long"
# The file statistics have a minimum 'z' followed by the actual minimum, the stored value of 2000 bytes.
${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('$DATA_DIR/duplicate_string_minimum.orc') WHERE s = repeat('b', 2000) SETTINGS input_format_orc_filter_push_down = 1"

echo "-- the statistics of the Java writer that contain U+FFFD are cut before it"
# Written by Apache ORC Java 2.2.1: row groups of 1000 rows, each with 999 values 'a' and one long value. For the
# first three long values the Java writer stores an upper bound with U+FFFD that is less than the value. The
# sixth row group has one value 'b\x80', stored as the minimum 'b' U+FFFD that is greater than the value, and a long
# value. In the last row group the Java writer increments the invalid UTF-8 at the end of the long value into U+FFFE.
for condition in \
    "s = concat(repeat('b', 1020), unhex('F48FBFBF'), 'tail')" \
    "s = concat(repeat('b', 1021), unhex('EFBFBE'), 'tail')" \
    "s = concat(repeat('b', 1000), unhex(repeat('FF', 50)))" \
    "s = repeat('b', 2000)" \
    "s = repeat('c', 3000)" \
    "s = unhex('6280')" \
    "s = concat(repeat('b', 1021), unhex('FF8080'), 'tail')"
do
    query "$DATA_DIR/java_unsafe_string_bounds.orc" "$condition" _row_number
done
