#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database

# STREAM from a `FileLog` table reads the files without consuming them.
# Its cursor is {file name: <offset of the last read record>}, where a dot in a file name is written as %2E.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "$dir"
mkdir -p "$dir"

printf '1,a\n2,b\n3,c\n' > "$dir/a.log"
printf '10,x\n20,y\n' > "$dir/b.log"

$STREAMING_CLIENT -q "CREATE TABLE file_log (k UInt64, v String) ENGINE = FileLog('$dir', 'CSV') SETTINGS poll_timeout_ms = 100"

echo "--- bounded, no cursor"
$STREAMING_CLIENT -q "SELECT _filename, _offset, k, v FROM file_log STREAM BOUNDED ORDER BY ALL"

echo "--- bounded, with cursor"
$STREAMING_CLIENT -q "SELECT _filename, k FROM file_log STREAM BOUNDED CURSOR {'a%2Elog': 4, 'b%2Elog': 5} ORDER BY ALL"

echo "--- the STREAM does not consume the records"
$STREAMING_CLIENT -q "SELECT count() FROM file_log SETTINGS stream_like_engine_allow_direct_select = 1"

echo "--- a cursor outside a file"
$STREAMING_CLIENT -q "SELECT k FROM file_log STREAM BOUNDED CURSOR {'a%2Elog': 1000}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

echo "--- a dot in a file name that is not escaped"
$STREAMING_CLIENT -q "SELECT k FROM file_log STREAM BOUNDED CURSOR {'a.log': 4}" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

echo "--- unbounded, with cursor"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT _filename, k FROM file_log STREAM CURSOR {'a%2Elog': 8, 'b%2Elog': 5}")
printf '4,d\n' >> "$dir/a.log"
read_until "$fifo" "4"
# An incomplete record is not read until it is complete.
printf '5,' >> "$dir/a.log"
printf '30,z\n' > "$dir/c.log"
read_until "$fifo" "30"
printf 'e\n' >> "$dir/a.log"
read_until "$fifo" "5"
cleanup "$fifo" "$pid" > /dev/null

echo "--- incremental refresh"
refresh()
{
    $STREAMING_CLIENT -q "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv; SELECT '$1', count(), sum(k) FROM tgt"
}
$STREAMING_CLIENT -q "
    CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT k FROM file_log;
"
refresh round1
printf '6,f\n' >> "$dir/a.log"
refresh round2
# A file truncated in place is read from the beginning.
: > "$dir/b.log"
printf '7,g\n' >> "$dir/b.log"
refresh round3
refresh round4
# A file with a single long record truncated in place to a shorter record is read from the beginning.
printf '100000000,long\n' > "$dir/d.log"
refresh round5
printf '9,s\n' > "$dir/d.log"
refresh round6
$STREAMING_CLIENT -q "DROP TABLE mv"

$STREAMING_CLIENT -q "DROP TABLE file_log"

echo "--- file names with a common dotted prefix and with the suffixes of the structured cursor"
dir2=${dir}_names
rm -rf "$dir2"
mkdir -p "$dir2"
printf '1\n2\n' > "$dir2/app.log"
printf '3\n4\n' > "$dir2/app.log.1"
printf '5\n6\n' > "$dir2/app.offset"
printf '7\n8\n' > "$dir2/app.inode"
$STREAMING_CLIENT -q "CREATE TABLE file_log_names (k UInt64) ENGINE = FileLog('$dir2', 'CSV')"
$STREAMING_CLIENT -q "SELECT _filename, k FROM file_log_names STREAM BOUNDED CURSOR {'app%2Elog': 0, 'app%2Elog%2E1': 0, 'app%2Eoffset': 0, 'app%2Einode': 0} ORDER BY ALL"
$STREAMING_CLIENT -q "SELECT _filename, k FROM file_log_names STREAM BOUNDED CURSOR {'app%2Einode': 0, 'app%2Eoffset': 0, 'app%2Elog%2E1': 0, 'app%2Elog': 0} ORDER BY ALL"

echo "--- a file with two names (hard links) is read once"
ln "$dir2/app.log" "$dir2/hard.log"
$STREAMING_CLIENT -q "SELECT _filename, k FROM file_log_names STREAM BOUNDED ORDER BY ALL"
$STREAMING_CLIENT -q "DROP TABLE file_log_names"
rm -rf "$dir2"

echo "--- a record equal to the column name is not taken for a header"
dir3=${dir}_header
rm -rf "$dir3"
mkdir -p "$dir3"
printf 'a\nv\nb\n' > "$dir3/h.csv"
$STREAMING_CLIENT -q "CREATE TABLE file_log_header (v String) ENGINE = FileLog('$dir3', 'CSV')"
$STREAMING_CLIENT -q "SELECT v FROM file_log_header STREAM BOUNDED ORDER BY ALL"
$STREAMING_CLIENT -q "DROP TABLE file_log_header"
rm -rf "$dir3"

echo "--- a file renamed by log rotation keeps its position"
dir4=${dir}_rotation
rm -rf "$dir4"
mkdir -p "$dir4"
printf '1\n' > "$dir4/app.log"
$STREAMING_CLIENT -q "CREATE TABLE file_log_rotation (k UInt64) ENGINE = FileLog('$dir4', 'CSV') SETTINGS poll_timeout_ms = 100"
read -r fifo pid < <(spawn $STREAMING_CLIENT -q "SELECT _filename, k FROM file_log_rotation STREAM")
read_until "$fifo" "1"
mv "$dir4/app.log" "$dir4/app.log.1"
printf '2\n' > "$dir4/app.log"
read_until "$fifo" "2"
printf '3\n' >> "$dir4/app.log.1"
read_until "$fifo" "3"
cleanup "$fifo" "$pid" > /dev/null
$STREAMING_CLIENT -q "DROP TABLE file_log_rotation"
rm -rf "$dir4"

rm -rf "$dir"
